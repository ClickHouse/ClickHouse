from minio import Minio
import glob
import os
import json


def upload_directory(minio_client, bucket_name, local_path, s3_path):
    result_files = []
    for local_file in glob.glob(local_path + "/**"):
        if os.path.isfile(local_file):
            result_local_path = os.path.join(local_path, local_file)
            result_s3_path = os.path.join(s3_path, local_file)
            print(f"Putting file {result_local_path} to {result_s3_path}")
            minio_client.fput_object(
                bucket_name=bucket_name,
                object_name=result_s3_path,
                file_path=result_local_path,
            )
            result_files.append(result_s3_path)
        else:
            files = upload_directory(
                minio_client,
                bucket_name,
                os.path.join(local_path, local_file),
                os.path.join(s3_path, local_file),
            )
            result_files.extend(files)
    return result_files


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
