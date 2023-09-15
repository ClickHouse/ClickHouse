import logging
import pytest
import time
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.client import QueryRuntimeException

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/keeper_config.xml"], stay_alive=True, with_zookeeper=True
)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_merge_session_expired(started_cluster):
    node1.query("create table tab (x UInt64) engine = ReplicatedMergeTree('/clickhouse/tables/tab/', '0') order by tuple() settings old_parts_lifetime=3")
    node1.query("insert into tab select number from numbers(10)")
    node1.query("insert into tab select number + 10 from numbers(10)")
    node1.query("alter table tab delete where x = 12 settings mutations_sync=2")
    node1.query("alter table tab delete where x = 14 settings mutations_sync=2")
    node1.query("alter table tab delete where x = 16 settings mutations_sync=2")
    node1.query("system stop merges")
    # node1.query("insert into tab select number + 20 from numbers(10)")
    node1.query("optimize table tab final settings alter_sync=0")

    with PartitionManager() as pm:
        #logging.info(pm.dump_rules())
        pm.drop_instance_zk_connections(node1)
        node1.query("system start merges")
        node1.query("select sleep(1)")
        node1.restart_clickhouse()
        pm.restore_instance_zk_connections(node1)

    node1.query("system restart replica tab")
    node1.query("system sync replica tab")
    assert node1.query("select count() from tab") == '17\n'
