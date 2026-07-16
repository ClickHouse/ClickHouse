import time
import typing
import minio


def list_blobs(minio_client: minio.Minio, bucket="root", path="data/"):
    objects = minio_client.list_objects(bucket, path, recursive=True)
    return [obj.object_name for obj in objects]

def wait_blobs_synchronization(minio_client: minio.Minio, expected_objects_list: typing.List[str], bucket="root", path="data/"):
    print(f"Waiting blobs in {bucket}/{path} will be equal to {expected_objects_list}")

    for i in range(100):
        existing_objects = list_blobs(minio_client, bucket, path)
        print(f"[{i}] Existing S3 objects: {existing_objects}")

        if set(existing_objects) == set(expected_objects_list):
            print("Blobs set validated")
            return

        time.sleep(1)

    assert False, f"Failed to wait blobs synchronization: {set(existing_objects)} != {set(expected_objects_list)}"

def wait_blobs_count_synchronization(minio_client: minio.Minio, expected_objects_count: int, bucket="root", path="data/"):
    print(f"Waiting count of blobs in {bucket}/{path} will be equal to {expected_objects_count}")

    for i in range(100):
        existing_objects = list_blobs(minio_client, bucket, path)
        print(f"[{i}] Existing S3 objects: {existing_objects}")

        if len(existing_objects) == expected_objects_count:
            print("Blobs set validated")
            return

        time.sleep(1)

    assert False, f"Failed to wait blobs synchronization: {len(existing_objects)} != {expected_objects_count}"
