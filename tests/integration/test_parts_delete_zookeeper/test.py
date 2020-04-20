import time
import pytest

from helpers.network import PartitionManager
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml'], with_zookeeper=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        node1.query(
            '''
            CREATE DATABASE test;
            CREATE TABLE test_table(date Date, id UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/replicated', 'node1')
            ORDER BY id PARTITION BY toYYYYMM(date) SETTINGS old_parts_lifetime=4, cleanup_delay_period=1;
            '''
        )

        yield cluster

    except Exception as ex:
        print ex

    finally:
        cluster.shutdown()


# Test that outdated parts are not removed when they cannot be removed from zookeeper
def test_merge_doesnt_work_without_zookeeper(start_cluster):
    node1.query("INSERT INTO test_table VALUES ('2018-10-01', 1), ('2018-10-02', 2), ('2018-10-03', 3)")
    node1.query("INSERT INTO test_table VALUES ('2018-10-01', 4), ('2018-10-02', 5), ('2018-10-03', 6)")
    assert node1.query("SELECT count(*) from system.parts where table = 'test_table'") == "2\n"

    node1.query("OPTIMIZE TABLE test_table FINAL")
    assert node1.query("SELECT count(*) from system.parts where table = 'test_table'") == "3\n"

    assert_eq_with_retry(node1, "SELECT count(*) from system.parts where table = 'test_table' and active = 1", "1")

    node1.query("TRUNCATE TABLE test_table")

    assert node1.query("SELECT count(*) from system.parts where table = 'test_table'") == "0\n"

    node1.query("INSERT INTO test_table VALUES ('2018-10-01', 1), ('2018-10-02', 2), ('2018-10-03', 3)")
    node1.query("INSERT INTO test_table VALUES ('2018-10-01', 4), ('2018-10-02', 5), ('2018-10-03', 6)")
    assert node1.query("SELECT count(*) from system.parts where table = 'test_table'") == "2\n"

    with PartitionManager() as pm:
        node1.query("OPTIMIZE TABLE test_table FINAL")
        pm.drop_instance_zk_connections(node1)
        time.sleep(10) # > old_parts_lifetime
        assert node1.query("SELECT count(*) from system.parts where table = 'test_table'") == "3\n"

    assert_eq_with_retry(node1, "SELECT count(*) from system.parts where table = 'test_table' and active = 1", "1")
