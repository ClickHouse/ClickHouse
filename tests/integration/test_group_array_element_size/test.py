#!/usr/bin/env python3
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/group_array_max_element_size.xml"],
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/group_array_max_element_size.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test_max_exement_size(started_cluster):
    node1.query(
        "CREATE TABLE tab3 (x AggregateFunction(groupArray, Array(UInt8))) ENGINE = MergeTree ORDER BY tuple()"
    )
    node1.query("insert into tab3 select groupArrayState([zero]) from zeros(10)")
    assert node1.query("select length(groupArrayMerge(x)) from tab3") == "10\n"

    # First query should always fail
    with pytest.raises(Exception, match=r"Too large array size"):
        node1.query("insert into tab3 select groupArrayState([zero]) from zeros(11)")

    node1.replace_in_config(
        "/etc/clickhouse-server/config.d/group_array_max_element_size.xml",
        "10",
        "11",
    )

    node1.restart_clickhouse()

    node1.query("insert into tab3 select groupArrayState([zero]) from zeros(11)")
    assert node1.query("select length(groupArrayMerge(x)) from tab3") == "21\n"

    node1.replace_in_config(
        "/etc/clickhouse-server/config.d/group_array_max_element_size.xml",
        "11",
        "10",
    )

    node1.restart_clickhouse()

    with pytest.raises(Exception, match=r"Too large array size"):
        node1.query("select length(groupArrayMerge(x)) from tab3")

    node1.replace_in_config(
        "/etc/clickhouse-server/config.d/group_array_max_element_size.xml",
        "10",
        "11",
    )

    node1.restart_clickhouse()

    assert node1.query("select length(groupArrayMerge(x)) from tab3") == "21\n"


def test_limit_size(started_cluster):
    node2.query(
        "CREATE TABLE tab4 (x AggregateFunction(groupArray, Array(UInt8))) ENGINE = MergeTree ORDER BY tuple()"
    )
    node2.query("insert into tab4 select groupArrayState([zero]) from zeros(10)")
    assert node2.query("select length(groupArrayMerge(x)) from tab4") == "10\n"

    node2.replace_in_config(
        "/etc/clickhouse-server/config.d/group_array_max_element_size.xml",
        "throw",
        "discard",
    )

    node2.restart_clickhouse()

    node2.query("insert into tab4 select groupArrayState([zero]) from zeros(100)")
    assert node2.query("select length(groupArrayMerge(x)) from tab4") == "10\n"

    node2.replace_in_config(
        "/etc/clickhouse-server/config.d/group_array_max_element_size.xml",
        "discard",
        "throw",
    )

    node2.restart_clickhouse()

    with pytest.raises(Exception, match=r"Too large array size"):
        node2.query("insert into tab4 select groupArrayState([zero]) from zeros(11)")
