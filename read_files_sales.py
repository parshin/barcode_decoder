import logging
import time
import re
import requests
import json
import sys
import os
from os import listdir
from os.path import isfile, join
from pdf2image import convert_from_path
from conf import files_dir
from conf import addresses
from pyzbar.pyzbar import decode
from PIL import Image
from base64 import b64encode


def check_access():

    # Check permissions if we can read and write files to the directory
    if not os.access(files_dir['sales'], os.X_OK | os.W_OK):
        logging.error('Access is not allowed ' + files_dir['sales'])
        return False

    if not os.access(files_dir['recognized'], os.X_OK | os.W_OK):
        logging.error('Access is not allowed ' + files_dir['recognized'])
        return False

    if not os.access(files_dir['unrecognized'], os.X_OK | os.W_OK):
        logging.error('Access is not allowed ' + files_dir['unrecognized'])
        return False

    return True


if __name__ == "__main__":

    start_time = time.time()

    logging.basicConfig(filename=files_dir['sales_log'],
                        level=logging.INFO,
                        format='%(levelname)-8s [%(asctime)s] %(message)s')
    logging.info('start reading from: ' + files_dir["sales"])

    if not check_access():
        raise SystemExit(0)

    # pdf_file_list = glob.glob(files_dir["sales"]+"*.pdf")
    path = files_dir["sales"]
    pdf_file_list = [f for f in listdir(path) if isfile(join(path, f))]
    total_files = len(pdf_file_list)
    logging.info('total files: ' + str(total_files))
    recognized_files = 0

    for pdf_file in pdf_file_list:
        # filename = re.search(r'[^pdf\/].*[^.pdf]', pdf_file).group(0)
        filename = re.search(r'^.*(?=\.pdf)', pdf_file).group(0)

        jpg_file = convert_from_path(path + pdf_file, 200,
                                     output_folder=path,
                                     output_file=filename,
                                     fmt='jpg',
                                     thread_count=4,
                                     paths_only=True,
                                     first_page=1,
                                     last_page=1)

        detected_barcodes = decode(Image.open(jpg_file[0]))

        # no barcode data - move source pdf and delete temporary jpeg
        if not detected_barcodes:
            logging.error("barcode was n't recognized: " + pdf_file)
            try:
                os.rename(path+pdf_file, files_dir['unrecognized']+pdf_file)
                os.remove(jpg_file[0])
            except IOError:
                err_type, value, traceback = sys.exc_info()
                logging.error('error moving pdf file ' + pdf_file + " to " + files_dir['unrecognized'])
                logging.error('error detail:' + value.strerror)

            continue

        try:
            os.remove(jpg_file[0])
        except IOError:
            err_type, value, traceback = sys.exc_info()
            logging.error('error moving jpg file ' + jpg_file)
            logging.error('detail:' + value.strerror)

        for barcode in detected_barcodes:
            if barcode.type != 'CODE128':
                continue

            recognized_files += 1

            logging.info(' pdf_file ' + pdf_file +
                         ' type: ' + barcode.type +
                         ' barcode: ' + str(barcode.data))

            with open(path+pdf_file, 'rb') as f:

                base64_bytes = b64encode(f.read())
                base64_string = base64_bytes.decode('utf-8')

                response = requests.post(addresses["uf_address"], data=json.dumps({
                    'barcode': barcode.data.decode('utf-8'),
                    'file': base64_string,
                    'doc_type': 'sales',
                    'file_name': filename,
                }))

                f.close()

                if response.status_code != 200:
                    logging.error('error form 1c: ' + response.text)
                    continue

                try:
                    response = response.json()
                    if response["result"]:
                        logging.info(response["description"])
                        os.rename(path+pdf_file, files_dir['recognized']+pdf_file)
                    else:
                        logging.info('file not attached: ' + pdf_file)
                        os.rename(path+pdf_file, files_dir['unrecognized']+pdf_file)
                except IOError:
                    err_type, value, traceback = sys.exc_info()
                    logging.error('response form 1c: ' + str(response))
                    logging.error('error detail:' + value.strerror)

    logging.info('total files: ' + str(total_files))
    if total_files > 0:
        logging.info('recognized files: ' + str(recognized_files) + "/" +
                     str(round(recognized_files*100/total_files)) + "%")
    logging.info('execution time, sec.: ' + str(round(time.time() - start_time)))
