# A table that the server cannot load (here: a setting value written by a newer version, as after a rollback)
# must not stop the server's distributed DDL worker: ON CLUSTER queries and system.distributed_ddl_queue work.

import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    stay_alive=True,
    with_remote_database_disk=False,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_table_failed_to_load(started_cluster):
    db = "d_" + uuid.uuid4().hex[:8]
    timeout = {"distributed_ddl_task_timeout": 60}

    node1.query(f"CREATE DATABASE {db}")
    node1.query(f"CREATE TABLE {db}.good (x UInt64) ENGINE = MergeTree ORDER BY x")
    node1.query(f"INSERT INTO {db}.good VALUES (1)")
    node1.query(
        f"CREATE TABLE {db}.broken (x UInt64) ENGINE = MergeTree ORDER BY x "
        "SETTINGS object_shared_data_serialization_version = 'advanced'"
    )

    path = node1.query(
        f"SELECT metadata_path FROM system.tables WHERE database = '{db}' AND name = 'broken'"
    ).strip()
    if not path.startswith("/"):
        path = "/var/lib/clickhouse/" + path
    node1.stop_clickhouse()
    node1.exec_in_container(["sed", "-i", "s/'advanced'/'from_a_newer_version'/", path])
    assert "'from_a_newer_version'" in node1.exec_in_container(["cat", path])
    node1.start_clickhouse()

    error = node1.query_and_get_error(f"SELECT * FROM {db}.broken")
    assert "MergeTreeObjectSharedDataSerializationVersion" in error, error
    assert node1.query(f"SELECT * FROM {db}.good") == "1\n"

    node1.query("SELECT count() FROM system.distributed_ddl_queue")
    node1.query(
        f"CREATE TABLE {db}.t ON CLUSTER test_cluster (x UInt64) ENGINE = MergeTree ORDER BY x",
        settings=timeout,
    )
    assert node1.query(f"EXISTS TABLE {db}.t") == "1\n"
    assert (
        node1.query(
            "SELECT status FROM system.asynchronous_loader WHERE job = 'startup ddl worker'"
        )
        == "OK\n"
    )

    error = node1.query_and_get_error(
        f"CREATE TABLE {db}.broken ON CLUSTER test_cluster (x UInt64) ENGINE = MergeTree ORDER BY x",
        settings=timeout,
    )
    assert "TABLE_ALREADY_EXISTS" in error, error
    assert "'from_a_newer_version'" in node1.exec_in_container(["cat", path])
    node1.query(f"DROP TABLE {db}.t ON CLUSTER test_cluster SYNC", settings=timeout)
