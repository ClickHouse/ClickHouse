import glob
import json
import os
import shutil
from enum import Enum

from minio import Minio


class CloudUploader:
    def __init__(self, use_relpath=False):
        self.use_relpath = use_relpath

    def upload_directory(self, local_path, remote_blob_path, **kwargs):
        print(kwargs)
        result_files = []
        # print(f"Arguments: {local_path}, {remote_blob_path}")
        # for local_file in glob.glob(local_path + "/**"):
        #     print("Local file: {}", local_file)
        for local_file in glob.glob(local_path + "/**"):
            result_local_path = local_file
            result_remote_blob_path = os.path.join(
                remote_blob_path,
                (
                    os.path.relpath(local_file, start=local_path)
                    if self.use_relpath
                    else local_file
                ),
            )
            if os.path.isfile(local_file):
                self.upload_file(result_local_path, result_remote_blob_path, **kwargs)
                result_files.append(result_remote_blob_path)
            else:
                files = self.upload_directory(
                    result_local_path, result_remote_blob_path, **kwargs
                )
                result_files.extend(files)
        return result_files


class S3Uploader(CloudUploader):
    def __init__(self, minio_client, bucket_name, use_relpath=False):
        super().__init__(use_relpath=use_relpath)
        self.minio_client = minio_client
        self.bucket_name = bucket_name

    def upload_file(self, local_path, remote_blob_path, bucket=None):
        print(f"Upload to bucket: {bucket}")
        if bucket is None:
            bucket = self.bucket_name
        self.minio_client.fput_object(
            bucket_name=bucket,
            object_name=remote_blob_path,
            file_path=local_path,
        )

class S3Downloader:
    def __init__(self, minio_client, bucket_name):
        self.minio_client = minio_client
        self.bucket_name = bucket_name

    def download_file(self, remote_blob_path, local_path, bucket=None):
        print(f"Download from bucket: {bucket}")
        if bucket is None:
            bucket = self.bucket_name

        data = self.minio_client.get_object(
            bucket_name=bucket,
            object_name=remote_blob_path,
        )

        with open(local_path, "wb") as f:
            for chunk in data.stream():
                f.write(chunk)

    def download_directory(self, local_path, remote_blobs_path, **kwargs):
        if remote_blobs_path.startswith("/"):
            remote_blobs_path = remote_blobs_path[1:]

        objects = self.minio_client.list_objects(
            bucket_name=self.bucket_name,
            prefix=remote_blobs_path,
            recursive=True,
        )

        result_files = []
        for obj in objects:
            result_files.append(obj.object_name)
            diff_path = os.path.relpath(obj.object_name, start=remote_blobs_path)
            local_file_path = os.path.join(local_path, diff_path)
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            print(f"Downloading {obj.object_name} to {local_file_path}")
            self.download_file(obj.object_name, local_file_path, bucket=self.bucket_name)
        return result_files


class LocalUploader(CloudUploader):

    def __init__(self, clickhouse_node):
        super().__init__()
        self.clickhouse_node = clickhouse_node

    def upload_file(self, local_path, remote_blob_path):
        dir_path = os.path.dirname(remote_blob_path)
        if dir_path != "":
            self.clickhouse_node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "mkdir -p {}".format(dir_path),
                ]
            )
        self.clickhouse_node.copy_file_to_container(local_path, remote_blob_path)


class LocalDownloader:
    def __init__(self, clickhouse_node, use_relpath=False):
        self.use_relpath = use_relpath
        self.clickhouse_node = clickhouse_node

    def download_file(self, local_path, remote_blob_path):
        dir_path = os.path.dirname(local_path)
        if dir_path != "":
            self.clickhouse_node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "mkdir -p {}".format(dir_path),
                ]
            )
        print(f"Downloading {remote_blob_path} to {local_path}")
        self.clickhouse_node.copy_file_from_container(remote_blob_path, local_path)

    def download_directory(self, local_path, remote_blob_path, **kwargs):
        result_files = []
        for remote_file in self.clickhouse_node.get_files_list_in_container(remote_blob_path):
            result_remote_path = remote_file
            result_local_path = os.path.join(
                remote_blob_path,
                (
                    os.path.relpath(remote_file, start=local_path)
                    if self.use_relpath
                    else remote_file
                ),
            )
            if self.clickhouse_node.file_exists_in_container(result_remote_path):
                self.download_file(result_local_path, result_remote_path, **kwargs)
                result_files.append(result_local_path)
            else:
                files = self.download_directory(
                    result_local_path, result_local_path, **kwargs
                )
                result_files.extend(files)

        return result_files


class AzureUploader(CloudUploader):

    def __init__(self, blob_service_client, container_name):
        super().__init__()
        self.blob_service_client = blob_service_client
        self.container_client = self.blob_service_client.get_container_client(
            container_name
        )

    def upload_file(self, local_path, remote_blob_path, container_name=None):
        if container_name is None:
            container_client = self.container_client
        else:
            container_client = self.blob_service_client.get_container_client(
                container_name
            )
        blob_client = container_client.get_blob_client(remote_blob_path)
        with open(local_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)


def upload_directory(minio_client, bucket, local_path, remote_path, use_relpath=False):
    return S3Uploader(
        minio_client=minio_client, bucket_name=bucket, use_relpath=use_relpath
    ).upload_directory(local_path, remote_path)


def remove_directory(minio_client, bucket, remote_path):
    for obj in minio_client.list_objects(
        bucket, prefix=f"{remote_path}/", recursive=True
    ):
        minio_client.remove_object(bucket, obj.object_name)


def get_file_contents(minio_client, bucket, s3_path):
    data = minio_client.get_object(bucket, s3_path)
    data_str = b""
    for chunk in data.stream():
        data_str += chunk
    return data_str.decode()


def list_s3_objects(minio_client, bucket, prefix=""):
    prefix_len = len(prefix)
    return [
        obj.object_name[prefix_len:]
        for obj in minio_client.list_objects(bucket, prefix=prefix, recursive=True)
    ]


# Creates S3 bucket for tests and allows anonymous read-write access to it.
def prepare_s3_bucket(started_cluster):
    # Allows read-write access for bucket without authorization.
    bucket_read_write_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "",
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "s3:GetBucketLocation",
                "Resource": "arn:aws:s3:::root",
            },
            {
                "Sid": "",
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "s3:ListBucket",
                "Resource": "arn:aws:s3:::root",
            },
            {
                "Sid": "",
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::root/*",
            },
            {
                "Sid": "",
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "s3:PutObject",
                "Resource": "arn:aws:s3:::root/*",
            },
        ],
    }

    minio_client = started_cluster.minio_client
    minio_client.set_bucket_policy(
        started_cluster.minio_bucket, json.dumps(bucket_read_write_policy)
    )

    started_cluster.minio_restricted_bucket = "{}-with-auth".format(
        started_cluster.minio_bucket
    )
    if minio_client.bucket_exists(started_cluster.minio_restricted_bucket):
        minio_client.remove_bucket(started_cluster.minio_restricted_bucket)

    minio_client.make_bucket(started_cluster.minio_restricted_bucket)
