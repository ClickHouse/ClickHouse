from __future__ import annotations

import glob
import json
import os
from collections.abc import Iterator
from typing import Any, override

from minio import Minio
from minio.api import ObjectWriteResult
from minio.datatypes import Object
from urllib3.response import BaseHTTPResponse

from azure.storage.blob import BlobServiceClient

from .cluster import ClickHouseInstance, ClickHouseCluster

from azure.storage.blob import ContainerClient, BlobClient

class CloudUploader:
    def __init__(self, use_relpath: bool = False) -> None:
        self.use_relpath: bool = use_relpath

    def upload_file(self, local_path: str, remote_blob_path: str, **kwargs: Any) -> None:  # pyright: ignore[reportUnusedParameter, reportAny, reportExplicitAny]
        """Abstract method to be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement upload_file")

    def upload_directory(self, local_path: str, remote_blob_path: str, **kwargs: Any) -> list[str]:  # pyright: ignore[reportAny, reportExplicitAny]
        print(kwargs)
        result_files: list[str] = []
        # print(f"Arguments: {local_path}, {remote_blob_path}")
        # for local_file in glob.glob(local_path + "/**"):
        #     print("Local file: {}", local_file)
        for local_file in glob.glob(local_path + "/**"):
            result_local_path: str = local_file
            result_remote_blob_path: str = os.path.join(
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
                files: list[str] = self.upload_directory(
                    result_local_path, result_remote_blob_path, **kwargs
                )
                result_files.extend(files)
        return result_files


class S3Uploader(CloudUploader):
    def __init__(self, minio_client: Minio, bucket_name: str, use_relpath: bool = False) -> None:
        super().__init__(use_relpath=use_relpath)
        self.minio_client: Minio = minio_client
        self.bucket_name: str = bucket_name


    @override
    def upload_file(self, local_path: str, remote_blob_path: str, bucket: str | None = None, **kwargs: Any) -> None:  # pyright: ignore[reportAny, reportExplicitAny]
        print(f"Upload to bucket: {bucket}")
        if bucket is None:
            bucket = self.bucket_name
        _: ObjectWriteResult = self.minio_client.fput_object(
            bucket_name=bucket,
            object_name=remote_blob_path,
            file_path=local_path,
        )

class S3Downloader:
    def __init__(self, minio_client: Minio, bucket_name: str) -> None:
        self.minio_client: Minio = minio_client
        self.bucket_name: str = bucket_name

    def download_file(self, remote_blob_path: str, local_path: str, bucket: str | None = None) -> None:
        print(f"Download from bucket: {bucket}")
        if bucket is None:
            bucket = self.bucket_name

        data: BaseHTTPResponse = self.minio_client.get_object(
            bucket_name=bucket,
            object_name=remote_blob_path,
        )

        with open(local_path, "wb") as f:
            for chunk in data.stream():
                _ : int = f.write(chunk)

    def download_directory(self, local_path: str, remote_blobs_path: str, **_kwargs: Any) -> list[str]:  # pyright: ignore[reportAny, reportExplicitAny]
        if remote_blobs_path.startswith("/"):
            remote_blobs_path = remote_blobs_path[1:]

        objects: Iterator[Object] = self.minio_client.list_objects(
            bucket_name=self.bucket_name,
            prefix=remote_blobs_path,
            recursive=True,
        )

        result_files: list[str] = []
        for obj in objects:
            if obj.object_name is None:
                continue
            result_files.append(obj.object_name)
            diff_path: str = os.path.relpath(obj.object_name, start=remote_blobs_path)
            local_file_path: str = os.path.join(local_path, diff_path)
            _: str | None = os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            print(f"Downloading {obj.object_name} to {local_file_path}")
            self.download_file(obj.object_name, local_file_path, bucket=self.bucket_name)
        return result_files


class LocalUploader(CloudUploader):

    def __init__(self, clickhouse_node: ClickHouseInstance) -> None:
        super().__init__()
        self.clickhouse_node: ClickHouseInstance = clickhouse_node

    @override
    def upload_file(self, local_path: str, remote_blob_path: str, **_kwargs: Any) -> None: # pyright: ignore[reportAny, reportExplicitAny]
        dir_path: str = os.path.dirname(remote_blob_path)
        if dir_path != "":
            _: str = self.clickhouse_node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "mkdir -p {}".format(dir_path),
                ]
            )
        self.clickhouse_node.copy_file_to_container(local_path, remote_blob_path)


class LocalDownloader:
    def __init__(self, clickhouse_node: ClickHouseInstance, use_relpath: bool = False) -> None:
        self.use_relpath: bool = use_relpath
        self.clickhouse_node: ClickHouseInstance = clickhouse_node

    def download_file(self, local_path: str, remote_blob_path: str, **_kwargs: Any) -> None: # pyright: ignore[reportAny, reportExplicitAny]
        dir_path: str = os.path.dirname(local_path)
        if dir_path != "":
            _: str = self.clickhouse_node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "mkdir -p {}".format(dir_path),
                ]
            )
        print(f"Downloading {remote_blob_path} to {local_path}")
        self.clickhouse_node.copy_file_from_container(remote_blob_path, local_path)

    def download_directory(self, local_path: str, remote_blob_path: str, **kwargs: Any) -> list[str]: # pyright: ignore[reportAny, reportExplicitAny]
        result_files: list[str] = []
        for remote_file in self.clickhouse_node.get_files_list_in_container(remote_blob_path):
            result_remote_path: str = remote_file
            result_local_path: str = os.path.join(
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
                files: list[str] = self.download_directory(
                    result_local_path, result_local_path, **kwargs
                )
                result_files.extend(files)

        return result_files


class AzureUploader(CloudUploader):

    def __init__(self, blob_service_client: BlobServiceClient, container_name: str) -> None:
        super().__init__()
        self.blob_service_client: BlobServiceClient = blob_service_client
        self.container_client: ContainerClient = self.blob_service_client.get_container_client(
            container_name
        )

    @override
    def upload_file(self, local_path: str, remote_blob_path: str, container_name: str | None = None, **_kwargs: Any) -> None: # pyright: ignore[reportAny, reportExplicitAny]
        current_container_client: ContainerClient
        if container_name is None:
            current_container_client = self.container_client
        else:
            current_container_client = self.blob_service_client.get_container_client(
                container_name
            )
        blob_client: BlobClient = current_container_client.get_blob_client(remote_blob_path)
        with open(local_path, "rb") as data:
            _ : dict[str, Any] = blob_client.upload_blob(data, overwrite=True) # pyright: ignore[reportExplicitAny]


def upload_directory(minio_client: Minio, bucket: str, local_path: str, remote_path: str, use_relpath: bool = False) -> list[str]:
    return S3Uploader(
        minio_client=minio_client, bucket_name=bucket, use_relpath=use_relpath
    ).upload_directory(local_path, remote_path)


def remove_directory(minio_client: Minio, bucket: str, remote_path: str) -> None:
    for obj in minio_client.list_objects(
        bucket, prefix=f"{remote_path}/", recursive=True
    ):
        if obj.object_name is not None:
            minio_client.remove_object(bucket, obj.object_name)


def get_file_contents(minio_client: Minio, bucket: str, s3_path: str) -> str:
    data: BaseHTTPResponse = minio_client.get_object(bucket, s3_path)
    data_str: bytes = b""
    for chunk in data.stream():
        data_str += chunk
    return data_str.decode()


def list_s3_objects(minio_client: Minio, bucket: str, prefix: str = "") -> list[str]:
    prefix_len: int = len(prefix)
    return [
        obj.object_name[prefix_len:]
        for obj in minio_client.list_objects(bucket, prefix=prefix, recursive=True) 
        if obj.object_name is not None
    ]


# Creates S3 bucket for tests and allows anonymous read-write access to it.
def prepare_s3_bucket(started_cluster: ClickHouseCluster) -> None:
    # Allows read-write access for bucket without authorization.
    bucket_read_write_policy: dict[str, Any] = { # pyright: ignore[reportExplicitAny]
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

    minio_client: Minio = started_cluster.minio_client
    minio_client.set_bucket_policy(
        started_cluster.minio_bucket, json.dumps(bucket_read_write_policy)
    )

    restricted_bucket_name: str = "{}-with-auth".format(
        started_cluster.minio_bucket
    )
    if minio_client.bucket_exists(restricted_bucket_name):
        minio_client.remove_bucket(restricted_bucket_name)

    minio_client.make_bucket(restricted_bucket_name)
