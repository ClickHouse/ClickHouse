# A table that the server cannot load (here: a setting value written by a newer version, as after a rollback)
# must not make its Replicated database replica look lost: the replica keeps applying DDL and recovers around it.

import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    with_zookeeper=True,
    stay_alive=True,
    with_remote_database_disk=False,
    macros={"shard": 1, "replica": 1},
)
node2 = cluster.add_instance(
    "node2",
    with_zookeeper=True,
    stay_alive=True,
    with_remote_database_disk=False,
    macros={"shard": 1, "replica": 2},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def metadata_digest(zk_path):
    return node2.query(
        f"SELECT sum(sipHash64(concat(name, value))) FROM system.zookeeper WHERE path = '{zk_path}/metadata'"
    ).strip()


def assert_broken_table_fails_to_load(db):
    error = node1.query_and_get_error(f"SELECT * FROM {db}.broken")
    assert "MergeTreeObjectSharedDataSerializationVersion" in error, error
    assert node1.query(f"SELECT * FROM {db}.good") == "1\n"


def test_table_failed_to_load(started_cluster):
    db = "rdb_" + uuid.uuid4().hex[:8]
    zk_path = f"/clickhouse/databases/{db}"
    zk = cluster.get_kazoo_client("zoo1")
    r1 = f"{zk_path}/replicas/shard1|replica1"

    node1.query(
        f"CREATE DATABASE {db} ENGINE = Replicated('{zk_path}', 'shard1', 'replica1') SETTINGS max_broken_tables_ratio = 0.6"
    )
    node2.query(
        f"CREATE DATABASE {db} ENGINE = Replicated('{zk_path}', 'shard1', 'replica2')"
    )
    node1.query(f"CREATE TABLE {db}.good (x UInt64) ENGINE = MergeTree ORDER BY x")
    node1.query(f"INSERT INTO {db}.good VALUES (1)")
    node1.query(
        f"CREATE TABLE {db}.broken (x UInt64) ENGINE = MergeTree ORDER BY x "
        "SETTINGS object_shared_data_serialization_version = 'advanced'"
    )
    node2.query(f"SYSTEM SYNC DATABASE REPLICA {db}")
    assert zk.get(r1 + "/digest")[0].decode() == metadata_digest(zk_path)

    path = node1.query(
        f"SELECT metadata_path FROM system.tables WHERE database = '{db}' AND name = 'broken'"
    ).strip()
    if not path.startswith("/"):
        path = "/var/lib/clickhouse/" + path
    node1.stop_clickhouse()
    statement = zk.get(f"{zk_path}/metadata/broken")[0].decode()
    new_statement = statement.replace("'advanced'", "'from_a_newer_version'")
    assert new_statement != statement
    assert node1.exec_in_container(["cat", path]) == statement
    node1.exec_in_container(["sed", "-i", "s/'advanced'/'from_a_newer_version'/", path])
    assert node1.exec_in_container(["cat", path]) == new_statement
    zk.set(f"{zk_path}/metadata/broken", new_statement.encode())
    zk.set(r1 + "/digest", metadata_digest(zk_path).encode())
    node1.start_clickhouse()
    node1.query(
        "SYSTEM ENABLE FAILPOINT database_replicated_force_metadata_digest_check"
    )

    assert_broken_table_fails_to_load(db)

    node2.query(
        f"CREATE TABLE {db}.t1 (x UInt64) ENGINE = MergeTree ORDER BY x",
        settings={"distributed_ddl_task_timeout": 0},
    )
    assert_eq_with_retry(
        node1, f"EXISTS TABLE {db}.t1", "1", retry_count=60, sleep_time=1
    )
    node1.query(f"SYSTEM SYNC DATABASE REPLICA {db}")
    node1.query(f"CREATE TABLE {db}.t2 (x UInt64) ENGINE = MergeTree ORDER BY x")
    node2.query(f"SYSTEM SYNC DATABASE REPLICA {db}")
    assert node2.query(f"EXISTS TABLE {db}.t2") == "1\n"
    assert not node1.contains_in_log(f"DDLWorker({db}).*Replica seems to be lost")
    assert zk.get(r1 + "/digest")[0].decode() == metadata_digest(zk_path)

    node1.stop_clickhouse()
    zk.set(r1 + "/digest", b"42")
    # 2 stale tables of 4 (with the kept one): 4 * 0.6 >= 2, but 3 * 0.6 < 2 would refuse the recovery.
    node2.query(f"DROP TABLE {db}.t1", settings={"distributed_ddl_task_timeout": 0})
    node2.query(f"DROP TABLE {db}.t2", settings={"distributed_ddl_task_timeout": 0})
    node2.query(
        f"CREATE TABLE {db}.t3 (x UInt64) ENGINE = MergeTree ORDER BY x",
        settings={"distributed_ddl_task_timeout": 0},
    )
    node1.start_clickhouse()
    assert_eq_with_retry(
        node1, f"EXISTS TABLE {db}.t3", "1", retry_count=60, sleep_time=1
    )
    assert node1.contains_in_log(f"DatabaseReplicated ({db}).*Will recover replica")
    assert_broken_table_fails_to_load(db)
    assert (
        node1.query(
            f"SELECT count() FROM system.tables WHERE database = '{db}_broken_tables'"
        )
        == "2\n"
    )
    assert node1.query(f"EXISTS TABLE {db}.t1") == "0\n"
    node1.query(f"SYSTEM SYNC DATABASE REPLICA {db}")
    assert zk.get(r1 + "/digest")[0].decode() == metadata_digest(zk_path)
