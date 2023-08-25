import time

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

SETTINGS = "SETTINGS min_replicated_logs_to_keep=3, max_replicated_logs_to_keep=5, cleanup_delay_period=0, cleanup_delay_period_random_add=0"


def fill_nodes(nodes):
    for node in nodes:
        node.query(
            """
                CREATE TABLE test_table(date Date, id UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/replicated', '{replica}') ORDER BY id PARTITION BY toYYYYMM(date) 
                {settings};
            """.format(
                replica=node.name, settings=SETTINGS
            )
        )


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", with_zookeeper=True)
node2 = cluster.add_instance("node2", with_zookeeper=True)
node3 = cluster.add_instance("node3", with_zookeeper=True)
nodes = [node1, node2, node3]


def sync_replicas(table):
    for node in nodes:
        node.query("SYSTEM SYNC REPLICA {}".format(table))


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        fill_nodes([node1, node2, node3])

        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()


def test_recovery(start_cluster):
    node1.query("INSERT INTO test_table VALUES (1, 0)")
    sync_replicas("test_table")
    node2.query("DETACH TABLE test_table")

    for i in range(1, 11):
        node1.query("INSERT INTO test_table VALUES (1, {})".format(i))

    node2.query_with_retry(
        "ATTACH TABLE test_table",
        check_callback=lambda x: len(node2.query("select * from test_table")) > 0,
    )

    assert_eq_with_retry(
        node2,
        "SELECT count(*) FROM test_table",
        node1.query("SELECT count(*) FROM test_table"),
    )
    lost_marker = "Will mark replica node2 as lost"
    assert node1.contains_in_log(lost_marker) or node3.contains_in_log(lost_marker)

    sync_replicas("test_table")
    for node in nodes:
        assert (
            node.query("SELECT count(), sum(id) FROM test_table WHERE date=toDate(1)")
            == "11\t55\n"
        )


def test_choose_source_replica(start_cluster):
    node3.query("INSERT INTO test_table VALUES (2, 0)")
    sync_replicas("test_table")
    node2.query("DETACH TABLE test_table")
    node1.query(
        "SYSTEM STOP FETCHES test_table"
    )  # node1 will have many entries in queue, so node2 will clone node3

    for i in range(1, 11):
        node3.query("INSERT INTO test_table VALUES (2, {})".format(i))

    node2.query_with_retry(
        "ATTACH TABLE test_table",
        check_callback=lambda x: len(node2.query("select * from test_table")) > 0,
    )

    node1.query("SYSTEM START FETCHES test_table")
    node1.query("SYSTEM SYNC REPLICA test_table")
    node2.query("SYSTEM SYNC REPLICA test_table")

    assert node1.query("SELECT count(*) FROM test_table") == node3.query(
        "SELECT count(*) FROM test_table"
    )
    assert node2.query("SELECT count(*) FROM test_table") == node3.query(
        "SELECT count(*) FROM test_table"
    )

    lost_marker = "Will mark replica node2 as lost"
    assert node1.contains_in_log(lost_marker) or node3.contains_in_log(lost_marker)
    assert node2.contains_in_log("Will mimic node3")

    sync_replicas("test_table")
    for node in nodes:
        assert (
            node.query("SELECT count(), sum(id) FROM test_table WHERE date=toDate(2)")
            == "11\t55\n"
        )


def test_update_metadata(start_cluster):
    for node in nodes:
        node.query(
            """
                CREATE TABLE update_metadata(key UInt32)
                ENGINE = ReplicatedMergeTree('/test/update_metadata', '{replica}') ORDER BY key PARTITION BY key % 10
                {settings};
            """.format(
                replica=node.name, settings=SETTINGS
            )
        )

    for i in range(1, 11):
        node1.query("INSERT INTO update_metadata VALUES ({})".format(i))

    node2.query("DETACH TABLE update_metadata")
    # alter without mutation
    node1.query("ALTER TABLE update_metadata ADD COLUMN col1 UInt32")

    for i in range(1, 11):
        node1.query(
            "INSERT INTO update_metadata VALUES ({}, {})".format(i * 10, i * 10)
        )

    lost_marker = "Will mark replica node2 as lost"
    assert node1.contains_in_log(lost_marker) or node3.contains_in_log(lost_marker)

    node2.query("ATTACH TABLE update_metadata")
    sync_replicas("update_metadata")
    assert node1.query("DESC TABLE update_metadata") == node2.query(
        "DESC TABLE update_metadata"
    )
    assert node1.query("DESC TABLE update_metadata") == node3.query(
        "DESC TABLE update_metadata"
    )
    for node in nodes:
        assert (
            node.query("SELECT count(), sum(key), sum(col1) FROM update_metadata")
            == "20\t605\t550\n"
        )

    node2.query("DETACH TABLE update_metadata")
    # alter with mutation
    node1.query("ALTER TABLE update_metadata DROP COLUMN col1")
    for i in range(1, 11):
        node1.query("INSERT INTO update_metadata VALUES ({})".format(i * 100))

    lost_marker = "Will mark replica node2 as lost"
    assert node1.contains_in_log(lost_marker) or node3.contains_in_log(lost_marker)

    node2.query("ATTACH TABLE update_metadata")
    sync_replicas("update_metadata")
    assert node1.query("DESC TABLE update_metadata") == node2.query(
        "DESC TABLE update_metadata"
    )
    assert node1.query("DESC TABLE update_metadata") == node3.query(
        "DESC TABLE update_metadata"
    )

    # check that it's possible to execute alter on cloned replica
    node2.query("ALTER TABLE update_metadata ADD COLUMN col1 UInt32")
    sync_replicas("update_metadata")
    for node in nodes:
        assert (
            node.query("SELECT count(), sum(key), sum(col1) FROM update_metadata")
            == "30\t6105\t0\n"
        )

    # more complex case with multiple alters
    node2.query("TRUNCATE TABLE update_metadata")
    for i in range(1, 11):
        node1.query("INSERT INTO update_metadata VALUES ({}, {})".format(i, i))

    # The following alters hang because of "No active replica has part ... or covering part"
    # node2.query("SYSTEM STOP REPLICATED SENDS update_metadata")
    # node2.query("INSERT INTO update_metadata VALUES (42, 42)") # this part will be lost
    node2.query("DETACH TABLE update_metadata")

    node1.query("ALTER TABLE update_metadata MODIFY COLUMN col1 String")
    node1.query("ALTER TABLE update_metadata ADD COLUMN col2 INT")
    for i in range(1, 11):
        node3.query(
            "INSERT INTO update_metadata VALUES ({}, '{}', {})".format(
                i * 10, i * 10, i * 10
            )
        )
    node1.query("ALTER TABLE update_metadata DROP COLUMN col1")
    node1.query("ALTER TABLE update_metadata ADD COLUMN col3 Date")

    node2.query("ATTACH TABLE update_metadata")
    sync_replicas("update_metadata")
    assert node1.query("DESC TABLE update_metadata") == node2.query(
        "DESC TABLE update_metadata"
    )
    assert node1.query("DESC TABLE update_metadata") == node3.query(
        "DESC TABLE update_metadata"
    )
    for node in nodes:
        assert (
            node.query("SELECT count(), sum(key), sum(col2) FROM update_metadata")
            == "20\t605\t550\n"
        )
