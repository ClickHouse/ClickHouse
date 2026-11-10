import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key

cluster = ClickHouseCluster(__file__)

# 26.9.18.11 does not check SQL disks against `remote_url_allow_hosts`. The objects are created there with a list
# that does not name MinIO, then the server restarts on the current binary.
# Two instances: a failed check on the database metadata disk would stop the server.
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/remote_url_allow_hosts_without_minio.xml",
        "configs/drop_table_delay.xml",
    ],
    image="clickhouse/clickhouse-server",
    tag="26.9.18.11",
    with_installed_binary=True,
    with_minio=True,
    stay_alive=True,
)
node_db = cluster.add_instance(
    "node_db",
    main_configs=[
        "configs/remote_url_allow_hosts_without_minio.xml",
        "configs/drop_table_delay.xml",
    ],
    image="clickhouse/clickhouse-server",
    tag="26.9.18.11",
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
    # Detached permanently before the upgrade, so its disk is first built by the short ATTACH on the new binary.
    node.query(
        f"CREATE TABLE t_detached (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS disk = {s3_disk('detached_disk')}"
    )
    node.query("INSERT INTO t_detached VALUES (4), (5)")
    node.query("DETACH TABLE t_detached PERMANENTLY")
    # Dropped before the upgrade and still in the drop queue: the new binary rebuilds it at start from
    # metadata_dropped under FORCE_RESTORE, which is the path UNDROP relies on.
    node.query(
        f"CREATE TABLE t_dropped (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS disk = {s3_disk('dropped_disk')}"
    )
    node.query("INSERT INTO t_dropped VALUES (6), (7)")
    node.query("DROP TABLE t_dropped")

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
    # Two disks were built at start and reported: the live table's and the dropped table's.
    assert (
        node.query(
            "SELECT countSubstrings(message, '->') FROM system.warnings WHERE message LIKE '%remote_url_allow_hosts%'"
        )
        == "2\n"
    )
    node.query("UNDROP TABLE t_dropped")
    assert node.query("SELECT id FROM t_dropped ORDER BY id") == "6\n7\n"

    # Only loading existing metadata is exempt. A user ATTACH is checked like a CREATE, in the short form too:
    # the first build of the parked table's disk is rejected, like every other disk check on a short ATTACH.
    error = node.query_and_get_error("ATTACH TABLE t_detached")
    assert "UNACCEPTABLE_URL" in error, error
    error = node.query_and_get_error(
        f"CREATE TABLE t_new (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS disk = {s3_disk('new_disk')}"
    )
    assert "UNACCEPTABLE_URL" in error, error
    error = node.query_and_get_error(
        f"ATTACH TABLE t_attached UUID '{uuid.uuid4()}' (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS disk = {s3_disk('attached_disk')}"
    )
    assert "UNACCEPTABLE_URL" in error, error

    # The operator adds the entry that the warning names; the parked table attaches without a restart.
    node.replace_config(
        "/etc/clickhouse-server/config.d/remote_url_allow_hosts_without_minio.xml",
        "<clickhouse><remote_url_allow_hosts><host>allowed.example.com</host><s3_bucket>minio1:9001/root</s3_bucket></remote_url_allow_hosts></clickhouse>",
    )
    node.query("SYSTEM RELOAD CONFIG")
    node.query("ATTACH TABLE t_detached")
    assert node.query("SELECT id FROM t_detached ORDER BY id") == "4\n5\n"


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

    # DETACH and ATTACH reuse the metadata disk built at server start, so no check runs.
    node_db.query("DETACH DATABASE db")
    node_db.query("ATTACH DATABASE db")
    assert node_db.query("SELECT id FROM db.t ORDER BY id") == "1\n2\n3\n"
