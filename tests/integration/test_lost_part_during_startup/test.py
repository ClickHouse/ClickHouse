#!/usr/bin/env python3
import time

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", with_zookeeper=True, stay_alive=True)
node2 = cluster.add_instance("node2", with_zookeeper=True, stay_alive=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()


def remove_part_from_disk(node, table, part_name):
    part_path = node.query(
        "SELECT path FROM system.parts WHERE table = '{}' and name = '{}'".format(
            table, part_name
        )
    ).strip()
    if not part_path:
        raise Exception("Part " + part_name + "doesn't exist")
    node.exec_in_container(
        ["bash", "-c", "rm -r {p}/*".format(p=part_path)], privileged=True
    )


def test_lost_part_during_startup(start_cluster):
    for i, node in enumerate([node1, node2]):
        node.query(
            f"CREATE TABLE test_lost (value UInt64) Engine = ReplicatedMergeTree('/clickhouse/test_lost', '{i + 1}') ORDER BY tuple()"
        )

    for i in range(4):
        node2.query(f"INSERT INTO test_lost VALUES({i})")

    node2.query("OPTIMIZE TABLE test_lost FINAL")
    node1.query("SYSTEM SYNC REPLICA test_lost")

    assert (
        node2.query("SELECT sum(value) FROM test_lost")
        == str(sum(i for i in range(4))) + "\n"
    )
    assert (
        node1.query("SELECT sum(value) FROM test_lost")
        == str(sum(i for i in range(4))) + "\n"
    )

    remove_part_from_disk(node2, "test_lost", "all_0_3_1")
    remove_part_from_disk(node2, "test_lost", "all_1_1_0")
    remove_part_from_disk(node2, "test_lost", "all_2_2_0")

    node2.stop_clickhouse()
    node1.stop_clickhouse()
    node2.start_clickhouse()

    for i in range(10):
        try:
            node2.query("INSERT INTO test_lost VALUES(7)")
            node2.query("INSERT INTO test_lost VALUES(8)")
            node2.query("INSERT INTO test_lost VALUES(9)")
            node2.query("INSERT INTO test_lost VALUES(10)")
            node2.query("INSERT INTO test_lost VALUES(11)")
            node2.query("INSERT INTO test_lost VALUES(12)")

            node2.query("OPTIMIZE TABLE test_lost FINAL")
            break
        except Exception as ex:
            print("Exception", ex)
            time.sleep(0.5)

    node1.start_clickhouse()
    node2.query("SYSTEM SYNC REPLICA test_lost")
    node1.query("SYSTEM SYNC REPLICA test_lost")

    assert (
        node2.query("SELECT sum(value) FROM test_lost")
        == str(sum(i for i in range(4)) + sum(i for i in range(7, 13))) + "\n"
    )
    assert (
        node1.query("SELECT sum(value) FROM test_lost")
        == str(sum(i for i in range(4)) + sum(i for i in range(7, 13))) + "\n"
    )
