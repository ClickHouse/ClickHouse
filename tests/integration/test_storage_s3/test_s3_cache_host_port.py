import io
import os
import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key
from helpers.mock_servers import start_mock_servers

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", with_minio=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_schema_cache_key_distinguishes_host_and_port(started_cluster):
    # Two endpoints whose host and port concatenate to the same string: "s3h1" + "9001" and "s3h" + "19001".
    script_dir = os.path.join(os.path.dirname(__file__), "s3_mocks")
    start_mock_servers(started_cluster, script_dir, [("echo.py", "resolver", "19001")])
    resolver_ip = started_cluster.get_instance_ip("resolver")
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"echo '{started_cluster.minio_ip} s3h1' >> /etc/hosts && echo '{resolver_ip} s3h' >> /etc/hosts",
        ],
        user="root",
    )

    # The echo mock answers `/get-my-path/<key>` with one line, MinIO holds five lines under the same bucket and key.
    bucket = "get-my-path"
    key = f"{uuid.uuid4()}.csv"
    minio = started_cluster.minio_client
    if not minio.bucket_exists(bucket):
        minio.make_bucket(bucket)
    minio.put_object(bucket, key, io.BytesIO(b"1\n" * 5), 10)
    last_modified = minio.stat_object(bucket, key).last_modified.timestamp()
    while time.time() < last_modified + 2:
        time.sleep(0.5)

    auth = f"'minio', '{minio_secret_key}'"
    echo = f"s3('http://s3h:19001/{bucket}/{key}', {auth}, 'CSV', 'x String')"
    real = f"s3('http://s3h1:9001/{bucket}/{key}', {auth}, 'CSV', 'x String')"

    assert node.query(f"SELECT count() FROM {echo}") == "1\n"
    assert (
        node.query(f"SELECT count() FROM {real} SETTINGS use_cache_for_count_from_files = 0")
        == "5\n"
    )
    assert node.query(f"SELECT count() FROM {real}") == "5\n"
