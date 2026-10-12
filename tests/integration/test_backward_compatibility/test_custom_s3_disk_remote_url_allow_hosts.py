import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key

cluster = ClickHouseCluster(__file__)

# 26.8 does not check SQL disks against `remote_url_allow_hosts`. The objects are created there with a list
# that does not name MinIO, then the server restarts on the current binary.
# Two instances: a failed check on the database metadata disk would stop the server.
node = cluster.add_instance(
    "node",
    main_configs=["configs/remote_url_allow_hosts_without_minio.xml"],
    image="clickhouse/clickhouse-server",
    tag="26.8",
    with_installed_binary=True,
    with_minio=True,
    stay_alive=True,
)
node_db = cluster.add_instance(
    "node_db",
    main_configs=["configs/remote_url_allow_hosts_without_minio.xml"],
    image="clickhouse/clickhouse-server",
    tag="26.8",
    with_installed_binary=True,
    with_minio=True,
    stay_alive=True,
)

WARNING_IN_LOG = "is not allowed by `remote_url_allow_hosts`"


def s3_disk(path, disk_type="s3"):
    return (
        f"disk(type = '{disk_type}', endpoint = 'http://minio1:9001/root/data/{path}/', "
        f"access_key_id = 'minio', secret_access_key = '{minio_secret_key}')"
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_table_on_custom_s3_disk_loads_after_upgrade(started_cluster):
    node.query(
        f"CREATE TABLE t (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS disk = {s3_disk('upgrade_disk')}"
    )
    node.query("INSERT INTO t VALUES (1), (2), (3)")
    assert node.query("SELECT id FROM t ORDER BY id") == "1\n2\n3\n"
    # Detached before the upgrade, so its disk is first created by the short ATTACH on the new binary.
    node.query(
        f"CREATE TABLE t_detached (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS disk = {s3_disk('detached_disk')}"
    )
    node.query("INSERT INTO t_detached VALUES (4), (5)")
    node.query("DETACH TABLE t_detached PERMANENTLY")

    node.restart_with_latest_version()

    # Loaded from existing metadata: reported, not rejected.
    assert node.query("SELECT id FROM t ORDER BY id") == "1\n2\n3\n"
    assert node.contains_in_log(WARNING_IN_LOG)
    assert (
        node.query(
            "SELECT count() FROM system.warnings WHERE message LIKE '%remote_url_allow_hosts%' AND message LIKE '%minio1:9001/root%'"
        )
        == "1\n"
    )

    # A short ATTACH replays the stored definition: accepted and reported, like a server start.
    node.query("ATTACH TABLE t_detached")
    assert node.query("SELECT id FROM t_detached ORDER BY id") == "4\n5\n"
    assert (
        node.query(
            "SELECT count() FROM system.warnings WHERE message LIKE '%remote_url_allow_hosts%' AND message LIKE '%minio1:9001/root%'"
        )
        == "1\n"
    )

    # A query that defines a disk to the same endpoint is still rejected.
    error = node.query_and_get_error(
        f"CREATE TABLE t_new (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS disk = {s3_disk('new_disk')}"
    )
    assert "UNACCEPTABLE_URL" in error, error
    error = node.query_and_get_error(
        f"ATTACH TABLE t_attached UUID '{uuid.uuid4()}' (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS disk = {s3_disk('attached_disk')}"
    )
    assert "UNACCEPTABLE_URL" in error, error


def test_database_on_custom_s3_disk_loads_after_upgrade(started_cluster):
    node_db.query(
        f"CREATE DATABASE db ENGINE = Atomic SETTINGS disk = {s3_disk('upgrade_db_disk', 's3_plain_rewritable')}"
    )
    node_db.query("CREATE TABLE db.t (id UInt64) ENGINE = MergeTree ORDER BY id")
    node_db.query("INSERT INTO db.t VALUES (1), (2), (3)")
    assert node_db.query("SELECT id FROM db.t ORDER BY id") == "1\n2\n3\n"

    node_db.restart_with_latest_version()

    # The server starts and the database loads; the disk is reported.
    assert node_db.query("SELECT id FROM db.t ORDER BY id") == "1\n2\n3\n"
    assert node_db.contains_in_log(WARNING_IN_LOG)

    # A short ATTACH DATABASE replays the stored definition and is accepted.
    node_db.query("DETACH DATABASE db")
    node_db.query("ATTACH DATABASE db")
    assert node_db.query("SELECT id FROM db.t ORDER BY id") == "1\n2\n3\n"
