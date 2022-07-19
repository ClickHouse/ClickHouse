import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__, name="detach")
# Version 21.6.3.14 has incompatible partition id for tables with UUID in partition key.
node_21_6 = cluster.add_instance('node_21_6', image='yandex/clickhouse-server', tag='21.6.3.14', stay_alive=True, with_installed_binary=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()

def test_detach_part_wrong_partition_id(start_cluster):

    # Here we create table with partition by UUID.
    node_21_6.query("create table tab (id UUID, value UInt32) engine = MergeTree PARTITION BY (id) order by tuple()")
    node_21_6.query("insert into tab values ('61f0c404-5cb3-11e7-907b-a6006ad3dba0', 2)")

    # After restart, partition id will be different.
    # There is a single 0-level part, which will become broken.
    # We expect that it will not be removed (as usual for 0-level broken parts),
    # but moved to /detached
    node_21_6.restart_with_latest_version()

    num_detached = node_21_6.query("select count() from  system.detached_parts")
    assert num_detached == '1\n'
