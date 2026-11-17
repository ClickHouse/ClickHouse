import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/remote_url_allow_hosts.xml"],
    with_minio=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query(
            "CREATE TABLE t (id UInt64, s String) ENGINE = MergeTree ORDER BY id"
        )
        node.query("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        yield cluster
    finally:
        cluster.shutdown()


def test_backup_to_allowed_host():
    name = uuid.uuid4().hex
    destination = f"S3('http://minio1:9001/root/data/backups/{name}', 'minio', '{minio_secret_key}')"
    node.query(f"BACKUP TABLE t TO {destination}")
    node.query(f"RESTORE TABLE t AS t_{name} FROM {destination}")
    assert node.query(f"SELECT count() FROM t_{name}") == "2\n"
    node.query(f"DROP TABLE t_{name} SYNC")


@pytest.mark.parametrize(
    "url",
    [
        "http://minio1:9002/root/data/backups/x",
        "http://resolver:8080/root/data/backups/x",
        "http://127.0.0.1:9/root/data/backups/x",
    ],
)
def test_backup_to_disallowed_host(url):
    destination = f"S3('{url}', 'minio', '{minio_secret_key}')"
    settings = "SETTINGS backup_restore_s3_retry_attempts = 0"

    error = node.query_and_get_error(f"BACKUP TABLE t TO {destination} {settings}")
    assert "UNACCEPTABLE_URL" in error, error

    error = node.query_and_get_error(
        f"RESTORE TABLE t AS t_restored FROM {destination} {settings}"
    )
    assert "UNACCEPTABLE_URL" in error, error
