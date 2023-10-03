# -*- coding: utf-8 -*-
import hashlib
import logging
import re
import shutil
import time
from multiprocessing.dummy import Pool
from pathlib import Path
from typing import List, Union

import boto3  # type: ignore
import botocore  # type: ignore

from env_helper import (
    S3_TEST_REPORTS_BUCKET,
    S3_BUILDS_BUCKET,
    RUNNER_TEMP,
    CI,
    S3_URL,
    S3_DOWNLOAD,
)
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
    max_pool_size = 100

    def __init__(self):
        config = botocore.config.Config(max_pool_connections=self.max_pool_size)
        self.session = boto3.session.Session(region_name="us-east-1")
        self.client = self.session.client("s3", endpoint_url=S3_URL, config=config)
        self.host = S3_URL

    def _upload_file_to_s3(
        self, bucket_name: str, file_path: Path, s3_path: str
    ) -> str:
        logging.debug(
            "Start uploading %s to bucket=%s path=%s", file_path, bucket_name, s3_path
        )
        metadata = {}
        if file_path.stat().st_size < 64 * 1024 * 1024:
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
                logging.info("No content type provided for %s", file_path)
        else:
            if re.search(r"\.(txt|log|err|out)$", s3_path) or re.search(
                r"\.log\..*(?<!\.zst)$", s3_path
            ):
                compressed_path = file_path.with_suffix(file_path.suffix + ".zst")
                logging.info(
                    "Going to compress file log file %s to %s",
                    file_path,
                    compressed_path,
                )
                compress_file_fast(file_path, compressed_path)
                file_path = compressed_path
                s3_path += ".zst"
            else:
                logging.info("Processing file without compression")
            logging.info("File is too large, do not provide content type")

        self.client.upload_file(file_path, bucket_name, s3_path, ExtraArgs=metadata)
        url = self.s3_url(bucket_name, s3_path)
        logging.info("Upload %s to %s. Meta: %s", file_path, url, metadata)
        return url

    def upload_test_report_to_s3(self, file_path: Path, s3_path: str) -> str:
        if CI:
            return self._upload_file_to_s3(S3_TEST_REPORTS_BUCKET, file_path, s3_path)

        return S3Helper.copy_file_to_local(S3_TEST_REPORTS_BUCKET, file_path, s3_path)

    def upload_build_file_to_s3(self, file_path: Path, s3_path: str) -> str:
        if CI:
            return self._upload_file_to_s3(S3_BUILDS_BUCKET, file_path, s3_path)

        return S3Helper.copy_file_to_local(S3_BUILDS_BUCKET, file_path, s3_path)

    def fast_parallel_upload_dir(
        self, dir_path: Path, s3_dir_path: str, bucket_name: str
    ) -> List[str]:
        all_files = [file for file in dir_path.rglob("*") if file.is_file()]

        logging.info("Files found %s", len(all_files))

        counter = 0
        t = time.time()
        sum_time = 0

        def upload_task(file_path: Path) -> str:
            nonlocal counter
            nonlocal t
            nonlocal sum_time
            file_str = file_path.as_posix()
            try:
                s3_path = file_str.replace(str(dir_path), s3_dir_path)
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
                        f"Uploaded {counter}, {int(time.time()-t)}s, "
                        f"sum time {sum_time}s",
                    )
                    t = time.time()
            except Exception as ex:
                logging.critical("Failed to upload file, expcetion %s", ex)
            return self.s3_url(bucket_name, s3_path)

        p = Pool(self.max_pool_size)

        original_level = logging.root.level
        logging.basicConfig(level=logging.CRITICAL)
        result = sorted(_flatten_list(p.map(upload_task, all_files)))
        logging.basicConfig(level=original_level)
        return result

    def _upload_directory_to_s3(
        self,
        directory_path: Path,
        s3_directory_path: str,
        bucket_name: str,
        keep_dirs_in_s3_path: bool,
        upload_symlinks: bool,
    ) -> List[str]:
        logging.info(
            "Upload directory '%s' to bucket=%s of s3 directory '%s'",
            directory_path,
            bucket_name,
            s3_directory_path,
        )
        if not directory_path.exists():
            return []
        files = list(directory_path.iterdir())
        if not files:
            return []

        p = Pool(min(len(files), 5))

        def task(file_path: Path) -> Union[str, List[str]]:
            full_fs_path = file_path.absolute()
            if keep_dirs_in_s3_path:
                full_s3_path = "/".join((s3_directory_path, directory_path.name))
            else:
                full_s3_path = s3_directory_path

            if full_fs_path.is_dir():
                return self._upload_directory_to_s3(
                    full_fs_path,
                    full_s3_path,
                    bucket_name,
                    keep_dirs_in_s3_path,
                    upload_symlinks,
                )

            if full_fs_path.is_symlink():
                if upload_symlinks:
                    if CI:
                        return self._upload_file_to_s3(
                            bucket_name,
                            full_fs_path,
                            full_s3_path + "/" + file_path.name,
                        )
                    return S3Helper.copy_file_to_local(
                        bucket_name, full_fs_path, full_s3_path + "/" + file_path.name
                    )
                return []

            if CI:
                return self._upload_file_to_s3(
                    bucket_name, full_fs_path, full_s3_path + "/" + file_path.name
                )

            return S3Helper.copy_file_to_local(
                bucket_name, full_fs_path, full_s3_path + "/" + file_path.name
            )

        return sorted(_flatten_list(list(p.map(task, files))))

    def upload_build_directory_to_s3(
        self,
        directory_path: Path,
        s3_directory_path: str,
        keep_dirs_in_s3_path: bool = True,
        upload_symlinks: bool = True,
    ) -> List[str]:
        return self._upload_directory_to_s3(
            directory_path,
            s3_directory_path,
            S3_BUILDS_BUCKET,
            keep_dirs_in_s3_path,
            upload_symlinks,
        )

    def upload_test_directory_to_s3(
        self,
        directory_path: Path,
        s3_directory_path: str,
        keep_dirs_in_s3_path: bool = True,
        upload_symlinks: bool = True,
    ) -> List[str]:
        return self._upload_directory_to_s3(
            directory_path,
            s3_directory_path,
            S3_TEST_REPORTS_BUCKET,
            keep_dirs_in_s3_path,
            upload_symlinks,
        )

    def list_prefix(
        self, s3_prefix_path: str, bucket: str = S3_BUILDS_BUCKET
    ) -> List[str]:
        objects = self.client.list_objects_v2(Bucket=bucket, Prefix=s3_prefix_path)
        result = []
        if "Contents" in objects:
            for obj in objects["Contents"]:
                result.append(obj["Key"])

        return result

    def url_if_exists(self, key: str, bucket: str = S3_BUILDS_BUCKET) -> str:
        if not CI:
            local_path = self.local_path(bucket, key)
            if local_path.exists():
                return local_path.as_uri()
            return ""

        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return self.s3_url(bucket, key)
        except Exception:
            return ""

    @staticmethod
    def get_url(bucket: str, key: str) -> str:
        if CI:
            return S3Helper.s3_url(bucket, key)
        return S3Helper.local_path(bucket, key).as_uri()

    @staticmethod
    def s3_url(bucket: str, key: str) -> str:
        url = f"{S3_DOWNLOAD}/{bucket}/{key}"
        # last two replacements are specifics of AWS urls:
        # https://jamesd3142.wordpress.com/2018/02/28/amazon-s3-and-the-plus-symbol/
        url = url.replace("+", "%2B").replace(" ", "%20")
        return url

    @staticmethod
    def local_path(bucket: str, key: str) -> Path:
        return (Path(RUNNER_TEMP) / "s3" / bucket / key).absolute()

    @staticmethod
    def copy_file_to_local(bucket_name: str, file_path: Path, s3_path: str) -> str:
        local_path = S3Helper.local_path(bucket_name, s3_path)
        local_dir = local_path.parent
        local_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy(file_path, local_path)

        logging.info("Copied %s to %s", file_path, local_path)
        return local_path.as_uri()
