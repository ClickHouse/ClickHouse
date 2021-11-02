# -*- coding: utf-8 -*-
import hashlib
import logging
import os
from multiprocessing.dummy import Pool
import boto3
from compress_files import compress_file_fast

def _md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    logging.debug("MD5 for %s is %s", fname, hash_md5.hexdigest())
    return hash_md5.hexdigest()


def _flatten_list(lst):
    result = []
    for elem in lst:
        if isinstance(elem, list):
            result += _flatten_list(elem)
        else:
            result.append(elem)
    return result


class S3Helper():
    def __init__(self, host):
        self.session = boto3.session.Session(region_name='us-east-1')
        self.client = self.session.client('s3', endpoint_url=host)

    def _upload_file_to_s3(self, bucket_name, file_path, s3_path):
        logging.debug("Start uploading %s to bucket=%s path=%s", file_path, bucket_name, s3_path)
        metadata = {}
        if os.path.getsize(file_path) < 64 * 1024 * 1024:
            if s3_path.endswith("txt") or s3_path.endswith("log") or s3_path.endswith("err") or s3_path.endswith("out"):
                metadata['ContentType'] = "text/plain; charset=utf-8"
                logging.info("Content type %s for file path %s", "text/plain; charset=utf-8", file_path)
            elif s3_path.endswith("html"):
                metadata['ContentType'] = "text/html; charset=utf-8"
                logging.info("Content type %s for file path %s", "text/html; charset=utf-8", file_path)
            else:
                logging.info("No content type provied for %s", file_path)
        else:
            if s3_path.endswith("txt") or s3_path.endswith("log") or s3_path.endswith("err") or s3_path.endswith("out"):
                logging.info("Going to compress file log file %s to %s", file_path, file_path + ".gz")
                compress_file_fast(file_path, file_path + ".gz")
                file_path += ".gz"
                s3_path += ".gz"
            else:
                logging.info("Processing file without compression")
            logging.info("File is too large, do not provide content type")

        self.client.upload_file(file_path, bucket_name, s3_path, ExtraArgs=metadata)
        logging.info("Upload %s to %s. Meta: %s", file_path, s3_path, metadata)
        # last two replacements are specifics of AWS urls: https://jamesd3142.wordpress.com/2018/02/28/amazon-s3-and-the-plus-symbol/
        return "https://s3.amazonaws.com/{bucket}/{path}".format(bucket=bucket_name, path=s3_path).replace('+', '%2B').replace(' ', '%20')

    def upload_test_report_to_s3(self, file_path, s3_path):
        return self._upload_file_to_s3('clickhouse-test-reports', file_path, s3_path)

    def upload_build_file_to_s3(self, file_path, s3_path):
        return self._upload_file_to_s3('clickhouse-builds', file_path, s3_path)

    def _upload_folder_to_s3(self, folder_path, s3_folder_path, bucket_name, keep_dirs_in_s3_path, upload_symlinks):
        logging.info("Upload folder '%s' to bucket=%s of s3 folder '%s'", folder_path, bucket_name, s3_folder_path)
        if not os.path.exists(folder_path):
            return []
        files = os.listdir(folder_path)
        if not files:
            return []

        p = Pool(min(len(files), 5))

        def task(file_name):
            full_fs_path = os.path.join(folder_path, file_name)
            if keep_dirs_in_s3_path:
                full_s3_path = s3_folder_path + "/" + os.path.basename(folder_path)
            else:
                full_s3_path = s3_folder_path

            if os.path.isdir(full_fs_path):
                return self._upload_folder_to_s3(full_fs_path, full_s3_path, bucket_name, keep_dirs_in_s3_path, upload_symlinks)

            if os.path.islink(full_fs_path):
                if upload_symlinks:
                    return self._upload_file_to_s3(bucket_name, full_fs_path, full_s3_path + "/" + file_name)
                return []

            return self._upload_file_to_s3(bucket_name, full_fs_path, full_s3_path + "/" + file_name)

        return sorted(_flatten_list(list(p.map(task, files))))

    def upload_build_folder_to_s3(self, folder_path, s3_folder_path, keep_dirs_in_s3_path=True, upload_symlinks=True):
        return self._upload_folder_to_s3(folder_path, s3_folder_path, 'clickhouse-builds', keep_dirs_in_s3_path, upload_symlinks)

    def upload_test_folder_to_s3(self, folder_path, s3_folder_path):
        return self._upload_folder_to_s3(folder_path, s3_folder_path, 'clickhouse-test-reports', True, True)
