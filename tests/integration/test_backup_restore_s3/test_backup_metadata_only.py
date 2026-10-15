import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/disk_s3.xml"],
    macros={"shard": "s1", "replica": "r1"},
    with_minio=True,
    with_zookeeper=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/disk_s3.xml"],
    macros={"shard": "s1", "replica": "r2"},
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

        # A `ReplicatedMergeTree` table on such a policy is refused from its definition too, unless its data is left out.
        node1.query(
            f"CREATE TABLE rdb.t_replicated (key Int) ENGINE = ReplicatedMergeTree ORDER BY key SETTINGS storage_policy = '{policy}'",
            settings={"distributed_ddl_task_timeout": 0},
        )
        assert node2.query("SELECT count() FROM system.tables WHERE database = 'rdb' AND name = 't_replicated'") == "0\n"
        backup_destination = f"S3('http://minio1:9001/root/data/backups/{uuid.uuid4().hex}', 'minio', '{minio_secret_key}')"
        err = node2.query_and_get_error(f"BACKUP TABLE rdb.t_replicated TO {backup_destination}")
        assert "NOT_IMPLEMENTED" in err and "ReplicatedMergeTree table on a storage policy marked backup_metadata_only" in err, err
        node2.query(f"BACKUP TABLE rdb.t_replicated TO {backup_destination} SETTINGS structure_only = 1")
    finally:
        node2.query("SYSTEM DISABLE FAILPOINT database_replicated_stop_entry_execution")
        for node in [node1, node2]:
            node.query("DROP DATABASE IF EXISTS rdb SYNC")


def test_disk_made_writable_by_config_reload(start_cluster):
    # The policy is checked when it is created, and the reload keeps it when its new version fails the check, but the
    # disk can still be made writable by the reload. Then the data written to it would be left out of the backup.
    policy = "policy_s3_plain_backup_metadata_only"
    config = "/etc/clickhouse-server/config.d/disk_s3.xml"
    node1.query(
        f"CREATE TABLE t_reload (key Int) ENGINE = MergeTree ORDER BY key SETTINGS storage_policy = '{policy}'"
    )
    node1.replace_in_config(config, "<readonly>true</readonly>", "<readonly>false</readonly>")
    try:
        node1.query("SYSTEM RELOAD CONFIG")
        err = node1.query_and_get_error(
            f"BACKUP TABLE t_reload TO S3('http://minio1:9001/root/data/backups/{uuid.uuid4().hex}', 'minio', '{minio_secret_key}')"
        )
        assert "INVALID_CONFIG_PARAMETER" in err and "is not read-only" in err, err
    finally:
        node1.replace_in_config(config, "<readonly>false</readonly>", "<readonly>true</readonly>")
        node1.query("SYSTEM RELOAD CONFIG")
        node1.query("DROP TABLE IF EXISTS t_reload SYNC")


def test_replicated_merge_tree_is_refused(start_cluster):
    # `RESTORE` creates a `ReplicatedMergeTree` table in a mode that refuses the parts it finds on the storage, so such a
    # table on a storage policy marked `backup_metadata_only` couldn't be restored from a backup of its definition.
    policy = "policy_s3_plain_backup_metadata_only"
    node1.query(
        f"""
        CREATE TABLE t_replicated (key Int) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid.uuid4().hex}', 'r1')
        ORDER BY key SETTINGS storage_policy = '{policy}'
        """
    )
    try:
        backup_destination = f"S3('http://minio1:9001/root/data/backups/{uuid.uuid4().hex}', 'minio', '{minio_secret_key}')"
        err = node1.query_and_get_error(f"BACKUP TABLE t_replicated TO {backup_destination}")
        assert "NOT_IMPLEMENTED" in err and "is a ReplicatedMergeTree table" in err, err
        node1.query(f"BACKUP TABLE t_replicated TO {backup_destination} SETTINGS structure_only = 1")
    finally:
        node1.query("DROP TABLE IF EXISTS t_replicated SYNC")
