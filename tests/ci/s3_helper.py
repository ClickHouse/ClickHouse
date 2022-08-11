# -*- coding: utf-8 -*-
import hashlib
import logging
import os
import re
import shutil
import time
from multiprocessing.dummy import Pool

import boto3  # type: ignore

from env_helper import S3_TEST_REPORTS_BUCKET, S3_BUILDS_BUCKET, RUNNER_TEMP, CI, S3_URL
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


class S3Helper:
    def __init__(self, host):
        self.session = boto3.session.Session(region_name="us-east-1")
        self.client = self.session.client("s3", endpoint_url=host)

    def _upload_file_to_s3(self, bucket_name, file_path, s3_path):
        logging.debug(
            "Start uploading %s to bucket=%s path=%s", file_path, bucket_name, s3_path
        )
        metadata = {}
        if os.path.getsize(file_path) < 64 * 1024 * 1024:
            if (
                s3_path.endswith("txt")
                or s3_path.endswith("log")
                or s3_path.endswith("err")
                or s3_path.endswith("out")
            ):
                metadata["ContentType"] = "text/plain; charset=utf-8"
                logging.info(
                    "Content type %s for file path %s",
                    "text/plain; charset=utf-8",
                    file_path,
                )
            elif s3_path.endswith("html"):
                metadata["ContentType"] = "text/html; charset=utf-8"
                logging.info(
                    "Content type %s for file path %s",
                    "text/html; charset=utf-8",
                    file_path,
                )
            elif s3_path.endswith("css"):
                metadata["ContentType"] = "text/css; charset=utf-8"
                logging.info(
                    "Content type %s for file path %s",
                    "text/css; charset=utf-8",
                    file_path,
                )
            elif s3_path.endswith("js"):
                metadata["ContentType"] = "text/javascript; charset=utf-8"
                logging.info(
                    "Content type %s for file path %s",
                    "text/css; charset=utf-8",
                    file_path,
                )
            else:
                logging.info("No content type provied for %s", file_path)
        else:
            if re.search(r"\.(txt|log|err|out)$", s3_path) or re.search(
                r"\.log\..*(?<!\.gz)$", s3_path
            ):
                logging.info(
                    "Going to compress file log file %s to %s",
                    file_path,
                    file_path + ".gz",
                )
                compress_file_fast(file_path, file_path + ".gz")
                file_path += ".gz"
                s3_path += ".gz"
            else:
                logging.info("Processing file without compression")
            logging.info("File is too large, do not provide content type")

        self.client.upload_file(file_path, bucket_name, s3_path, ExtraArgs=metadata)
        logging.info("Upload %s to %s. Meta: %s", file_path, s3_path, metadata)
        # last two replacements are specifics of AWS urls:
        # https://jamesd3142.wordpress.com/2018/02/28/amazon-s3-and-the-plus-symbol/
        url = f"{S3_URL}/{bucket_name}/{s3_path}"
        return url.replace("+", "%2B").replace(" ", "%20")

    def upload_test_report_to_s3(self, file_path, s3_path):
        if CI:
            return self._upload_file_to_s3(S3_TEST_REPORTS_BUCKET, file_path, s3_path)
        else:
            return S3Helper.copy_file_to_local(
                S3_TEST_REPORTS_BUCKET, file_path, s3_path
            )

    def upload_build_file_to_s3(self, file_path, s3_path):
        if CI:
            return self._upload_file_to_s3(S3_BUILDS_BUCKET, file_path, s3_path)
        else:
            return S3Helper.copy_file_to_local(S3_BUILDS_BUCKET, file_path, s3_path)

    def fast_parallel_upload_dir(self, dir_path, s3_dir_path, bucket_name):
        all_files = []

        for root, _, files in os.walk(dir_path):
            for file in files:
                all_files.append(os.path.join(root, file))

        logging.info("Files found %s", len(all_files))

        counter = 0
        t = time.time()
        sum_time = 0

        def upload_task(file_path):
            nonlocal counter
            nonlocal t
            nonlocal sum_time
            try:
                s3_path = file_path.replace(dir_path, s3_dir_path)
                metadata = {}
                if s3_path.endswith("html"):
                    metadata["ContentType"] = "text/html; charset=utf-8"
                elif s3_path.endswith("css"):
                    metadata["ContentType"] = "text/css; charset=utf-8"
                elif s3_path.endswith("js"):
                    metadata["ContentType"] = "text/javascript; charset=utf-8"

                # Retry
                for i in range(5):
                    try:
                        self.client.upload_file(
                            file_path, bucket_name, s3_path, ExtraArgs=metadata
                        )
                        break
                    except Exception as ex:
                        if i == 4:
                            raise ex
                        time.sleep(0.1 * i)

                counter += 1
                if counter % 1000 == 0:
                    sum_time += int(time.time() - t)
                    print(
                        "Uploaded",
                        counter,
                        "-",
                        int(time.time() - t),
                        "s",
                        "sum time",
                        sum_time,
                        "s",
                    )
                    t = time.time()
            except Exception as ex:
                logging.critical("Failed to upload file, expcetion %s", ex)
            return f"{S3_URL}/{bucket_name}/{s3_path}"

        p = Pool(256)

        logging.basicConfig(level=logging.CRITICAL)
        result = sorted(_flatten_list(p.map(upload_task, all_files)))
        logging.basicConfig(level=logging.INFO)
        return result

    def _upload_folder_to_s3(
        self,
        folder_path,
        s3_folder_path,
        bucket_name,
        keep_dirs_in_s3_path,
        upload_symlinks,
    ):
        logging.info(
            "Upload folder '%s' to bucket=%s of s3 folder '%s'",
            folder_path,
            bucket_name,
            s3_folder_path,
        )
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
                return self._upload_folder_to_s3(
                    full_fs_path,
                    full_s3_path,
                    bucket_name,
                    keep_dirs_in_s3_path,
                    upload_symlinks,
                )

            if os.path.islink(full_fs_path):
                if upload_symlinks:
                    if CI:
                        return self._upload_file_to_s3(
                            bucket_name, full_fs_path, full_s3_path + "/" + file_name
                        )
                    else:
                        return S3Helper.copy_file_to_local(
                            bucket_name, full_fs_path, full_s3_path + "/" + file_name
                        )
                return []

            if CI:
                return self._upload_file_to_s3(
                    bucket_name, full_fs_path, full_s3_path + "/" + file_name
                )
            else:
                return S3Helper.copy_file_to_local(
                    bucket_name, full_fs_path, full_s3_path + "/" + file_name
                )

        return sorted(_flatten_list(list(p.map(task, files))))

    def upload_build_folder_to_s3(
        self,
        folder_path,
        s3_folder_path,
        keep_dirs_in_s3_path=True,
        upload_symlinks=True,
    ):
        return self._upload_folder_to_s3(
            folder_path,
            s3_folder_path,
            S3_BUILDS_BUCKET,
            keep_dirs_in_s3_path,
            upload_symlinks,
        )

    def upload_test_folder_to_s3(
        self,
        folder_path,
        s3_folder_path,
        keep_dirs_in_s3_path=True,
        upload_symlinks=True,
    ):
        return self._upload_folder_to_s3(
            folder_path,
            s3_folder_path,
            S3_TEST_REPORTS_BUCKET,
            keep_dirs_in_s3_path,
            upload_symlinks,
        )

    def list_prefix(self, s3_prefix_path, bucket=S3_BUILDS_BUCKET):
        objects = self.client.list_objects_v2(Bucket=bucket, Prefix=s3_prefix_path)
        result = []
        if "Contents" in objects:
            for obj in objects["Contents"]:
                result.append(obj["Key"])

        return result

    @staticmethod
    def copy_file_to_local(bucket_name, file_path, s3_path):
        local_path = os.path.abspath(
            os.path.join(RUNNER_TEMP, "s3", bucket_name, s3_path)
        )
        local_dir = os.path.dirname(local_path)
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        shutil.copy(file_path, local_path)

        logging.info("Copied %s to %s", file_path, local_path)
        return f"file://{local_path}"
