import logging
import pytest
import time
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.client import QueryRuntimeException

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/keeper_config.xml", "configs/disks.xml"],
    stay_alive=True,
    with_zookeeper=True,
    with_minio=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_merge_session_expired(started_cluster):
    node1.query("drop table if exists tab")
    node1.query(
        "create table tab (x UInt64) engine = ReplicatedMergeTree('/clickhouse/tables/tab/', '0') order by tuple() settings old_parts_lifetime=3"
    )
    node1.query("insert into tab select number from numbers(10)")
    node1.query("insert into tab select number + 10 from numbers(10)")
    node1.query("alter table tab delete where x = 12 settings mutations_sync=2")
    node1.query("alter table tab delete where x = 14 settings mutations_sync=2")
    node1.query("alter table tab delete where x = 16 settings mutations_sync=2")
    node1.query("system stop merges")
    node1.query("optimize table tab final settings alter_sync=0")

    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(node1)
        node1.query("system start merges")
        node1.query("select sleep(1)")
        node1.restart_clickhouse()
        pm.restore_instance_zk_connections(node1)

    node1.query("system restart replica tab")
    assert node1.query("select count() from tab") == "17\n"


def test_merge_session_expired_zero_copy(started_cluster):
    node1.query("drop table if exists tab")
    node1.query(
        """
        create table tab (x UInt64, y UInt64) engine = ReplicatedMergeTree('/clickhouse/tables/tab2/', '0') order by tuple()
        settings old_parts_lifetime=1, storage_policy='s3', allow_remote_fs_zero_copy_replication=1, replicated_max_ratio_of_wrong_parts=1, min_bytes_for_wide_part=1
    """
    )

    node1.query("insert into tab select number, number from numbers(10)")
    node1.query("insert into tab select number + 10, number + 10 from numbers(10)")
    node1.query("alter table tab update x = x + 1 where 1 settings mutations_sync=2")
    node1.query("select * from tab")
    node1.query(
        "alter table tab update x = x + 1, y = y + 1 where 1 settings mutations_sync=2"
    )
    node1.query("select * from tab")
    node1.query("alter table tab update x = x + 1 where 1 settings mutations_sync=2")
    node1.query("select * from tab")

    node1.query(
        "alter table tab add column z UInt64 materialized x + sleepEachRow(0.05) settings mutations_sync=2"
    )
    node1.query("optimize table tab final settings alter_sync=0")

    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(node1)
        # Wait some time for merge to start
        # Part should be merged and stayed on disk, but not commited into zk
        node1.query("select sleep(2)")
        node1.restart_clickhouse()
        pm.restore_instance_zk_connections(node1)

    node1.query("system restart replica tab")
    # Wait for outdated parts to be removed
    node1.query("select sleep(3)")
    node1.query("select * from tab")
    node1.query("system sync replica tab")
    assert node1.query("select count() from tab") == "20\n"
