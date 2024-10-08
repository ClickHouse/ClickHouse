import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        node1.query(
            """
            CREATE DATABASE test;
            CREATE TABLE test_table(date Date, id UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/replicated', 'node1')
            ORDER BY id PARTITION BY toYYYYMM(date) SETTINGS old_parts_lifetime=4, cleanup_delay_period=1, cleanup_thread_preferred_points_per_iteration=0;
            """
        )

        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()


# Test that outdated parts are not removed when they cannot be removed from zookeeper
def test_merge_doesnt_work_without_zookeeper(start_cluster):
    node1.query(
        "INSERT INTO test_table VALUES ('2018-10-01', 1), ('2018-10-02', 2), ('2018-10-03', 3)"
    )
    node1.query(
        "INSERT INTO test_table VALUES ('2018-10-01', 4), ('2018-10-02', 5), ('2018-10-03', 6)"
    )
    assert (
        node1.query("SELECT count(*) from system.parts where table = 'test_table'")
        == "2\n"
    )

    node1.query("OPTIMIZE TABLE test_table FINAL")
    assert (
        node1.query("SELECT count(*) from system.parts where table = 'test_table'")
        == "3\n"
    )

    assert_eq_with_retry(
        node1,
        "SELECT count(*) from system.parts where table = 'test_table' and active = 1",
        "1",
    )

    node1.query("TRUNCATE TABLE test_table")

    total_parts = node1.query(
        "SELECT count(*) from system.parts where table = 'test_table'"
    )
    assert total_parts == "0\n" or total_parts == "1\n"

    assert (
        node1.query(
            "SELECT count(*) from system.parts where table = 'test_table' and active = 1"
        )
        == "0\n"
    )

    node1.query("DETACH TABLE test_table SYNC")
    node1.query("ATTACH TABLE test_table")

    node1.query(
        "INSERT INTO test_table VALUES ('2018-10-01', 1), ('2018-10-02', 2), ('2018-10-03', 3)"
    )
    node1.query(
        "INSERT INTO test_table VALUES ('2018-10-01', 4), ('2018-10-02', 5), ('2018-10-03', 6)"
    )
    assert (
        node1.query(
            "SELECT count(*) from system.parts where table = 'test_table' and active"
        )
        == "2\n"
    )

    # Parts may be moved to Deleting state and then back in Outdated state.
    # But system.parts returns only Active and Outdated parts if _state column is not queried.
    with PartitionManager() as pm:
        node1.query("OPTIMIZE TABLE test_table FINAL")
        pm.drop_instance_zk_connections(node1)
        # unfortunately we can be too fast and delete node before partition with ZK
        if (
            node1.query(
                "SELECT count(*) from system.parts where table = 'test_table' and _state!='dummy'"
            )
            == "1\n"
        ):
            print("We were too fast and deleted parts before partition with ZK")
        else:
            time.sleep(10)  # > old_parts_lifetime
            assert (
                node1.query(
                    "SELECT count(*) from system.parts where table = 'test_table' and _state!='dummy'"
                )
                == "3\n"
            )

    assert_eq_with_retry(
        node1,
        "SELECT count(*) from system.parts where table = 'test_table' and active = 1",
        "1",
    )
