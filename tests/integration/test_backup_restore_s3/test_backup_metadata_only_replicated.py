import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/disk_s3.xml"],
    with_minio=True,
    with_zookeeper=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/disk_s3.xml"],
    with_minio=True,
    with_zookeeper=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


def test_partitions_of_table_not_created_on_replica(start_cluster):
    # A replica of a `Replicated` database backs up a table from its definition in Keeper before the table is created on
    # the replica. A backup of some partitions of a table on a storage policy marked `backup_metadata_only` is refused from
    # the definition then, as it would be restored as the whole table.
    policy = "policy_s3_plain_backup_metadata_only"
    zookeeper_path = f"/clickhouse/databases/rdb_{uuid.uuid4().hex}"
    for node, replica in [(node1, "r1"), (node2, "r2")]:
        node.query(f"CREATE DATABASE rdb ENGINE = Replicated('{zookeeper_path}', 's1', '{replica}')")

    node2.query("SYSTEM ENABLE FAILPOINT database_replicated_stop_entry_execution")
    try:
        node1.query(
            f"""
            CREATE TABLE rdb.t (key Int, value String) ENGINE = MergeTree PARTITION BY key % 2 ORDER BY key
            SETTINGS storage_policy = '{policy}'
            """,
            settings={"distributed_ddl_task_timeout": 0},
        )
        assert node2.query("SELECT count() FROM system.tables WHERE database = 'rdb' AND name = 't'") == "0\n"

        err = node2.query_and_get_error(
            f"BACKUP TABLE rdb.t PARTITIONS 0 TO S3('http://minio1:9001/root/data/backups/{uuid.uuid4().hex}', 'minio', '{minio_secret_key}')"
        )
        assert "CANNOT_BACKUP_TABLE" in err and "Cannot backup specific partitions" in err, err
    finally:
        node2.query("SYSTEM DISABLE FAILPOINT database_replicated_stop_entry_execution")
        for node in [node1, node2]:
            node.query("DROP DATABASE IF EXISTS rdb SYNC")
