import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

# Regression test for BACKUP / RESTORE ON CLUSTER when a replica has skip_distributed_ddl=1.
#
# BACKUP / RESTORE ON CLUSTER dispatch their internal queries through executeDDLQueryOnCluster, which
# excludes skip_distributed_ddl replicas. The coordination host set (all_hosts) and the RESTORE access
# precheck must be kept consistent with that dispatch set: otherwise the initiator waits for the
# skipped host until timeout (it never joins coordination) and the precheck may veto the restore.
#
# node2 is up but excluded via skip_distributed_ddl, so only node1 takes part in both flows.

cluster = ClickHouseCluster(__file__)
COMMON_CONFIGS = ["configs/remote_servers.xml", "configs/backups_disk.xml"]
node1 = cluster.add_instance("node1", main_configs=COMMON_CONFIGS, with_zookeeper=True)
node2 = cluster.add_instance("node2", main_configs=COMMON_CONFIGS, with_zookeeper=True)

DDL_SETTINGS = {"distributed_ddl_task_timeout": 60}


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_backup_restore_on_cluster_with_skipped_replica(started_cluster):
    node1.query(
        "DROP TABLE IF EXISTS tbl ON CLUSTER 'backup_cluster' SYNC", settings=DDL_SETTINGS
    )
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'backup_cluster' (n Int32) ENGINE=MergeTree ORDER BY n",
        settings=DDL_SETTINGS,
    )
    node1.query("INSERT INTO tbl VALUES (1), (2), (3)")

    backup_name = "Disk('backups', 'skip_distributed_ddl_backup')"

    # Must complete rather than hang waiting for the skipped node2 in the coordination host set.
    assert "BACKUP_CREATED" in node1.query(
        f"BACKUP TABLE tbl ON CLUSTER 'backup_cluster' TO {backup_name}"
    )

    node1.query("DROP TABLE tbl ON CLUSTER 'backup_cluster' SYNC", settings=DDL_SETTINGS)

    # RESTORE must not be vetoed by the skipped node2's access precheck and must complete.
    assert "RESTORED" in node1.query(
        f"RESTORE TABLE tbl ON CLUSTER 'backup_cluster' FROM {backup_name}"
    )

    assert node1.query("SELECT count() FROM tbl").strip() == "3"
    # node2 is excluded from ON CLUSTER DDL, so it never received the table.
    assert (
        node2.query("SELECT count() FROM system.tables WHERE name = 'tbl'").strip() == "0"
    )


def test_backup_restore_on_cluster_all_skipped_fails_early(started_cluster):
    node1.query(
        "DROP TABLE IF EXISTS tbl2 ON CLUSTER 'backup_cluster' SYNC", settings=DDL_SETTINGS
    )
    node1.query(
        "CREATE TABLE tbl2 ON CLUSTER 'backup_cluster' (n Int32) ENGINE=MergeTree ORDER BY n",
        settings=DDL_SETTINGS,
    )
    node1.query("INSERT INTO tbl2 VALUES (1)")

    backup_name = "Disk('backups', 'all_skipped_backup')"

    # replica_num=2 selects the skipped node2, so the effective host set is empty. The BAD_ARGUMENTS
    # error must be raised before the backup destination is opened, so no backup artifacts are left.
    with pytest.raises(QueryRuntimeException, match="skip_distributed_ddl"):
        node1.query(
            f"BACKUP TABLE tbl2 ON CLUSTER 'backup_cluster' TO {backup_name} SETTINGS replica_num=2"
        )
    assert (
        node1.exec_in_container(
            ["bash", "-c", "test -e /backups/all_skipped_backup && echo present || echo absent"]
        ).strip()
        == "absent"
    )

    # RESTORE with an all-skipped selection must fail with the config error before opening the backup
    # source - otherwise it would fail later with a different "backup not found" error.
    with pytest.raises(QueryRuntimeException, match="skip_distributed_ddl"):
        node1.query(
            f"RESTORE TABLE tbl2 ON CLUSTER 'backup_cluster' FROM {backup_name} SETTINGS replica_num=2"
        )

    node1.query("DROP TABLE tbl2 ON CLUSTER 'backup_cluster' SYNC", settings=DDL_SETTINGS)
