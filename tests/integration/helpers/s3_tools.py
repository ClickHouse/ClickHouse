import glob
import json
import os
import shutil
from enum import Enum

from minio import Minio
from pyhdfs import HdfsClient


class CloudUploader:

    def upload_directory(self, local_path, remote_blob_path, **kwargs):
        print(kwargs)
        result_files = []
        # print(f"Arguments: {local_path}, {s3_path}")
        # for local_file in glob.glob(local_path + "/**"):
        #     print("Local file: {}", local_file)
        for local_file in glob.glob(local_path + "/**"):
            result_local_path = os.path.join(local_path, local_file)
            result_remote_blob_path = os.path.join(remote_blob_path, local_file)
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
    def __init__(self, minio_client, bucket_name):
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


class LocalUploader(CloudUploader):

    def __init__(self, clickhouse_node):
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


class HDFSUploader(CloudUploader):

    def __init__(self, started_cluster):
        self.started_cluster = started_cluster

    def upload_file(self, local_path, remote_blob_path):
        dir_path = os.path.dirname(remote_blob_path)
        fs = HdfsClient(hosts=self.started_cluster.hdfs_ip)

        exists = fs.exists(dir_path)
        if not exists:
            fs.mkdirs(dir_path)

        hdfs_api = self.started_cluster.hdfs_api
        hdfs_api.write_file(remote_blob_path, local_path)


class AzureUploader(CloudUploader):

    def __init__(self, blob_service_client, container_name):
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


def upload_directory(minio_client, bucket, local_path, remote_path):
    return S3Uploader(minio_client=minio_client, bucket_name=bucket).upload_directory(
        local_path, remote_path
    )


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
