# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node", stay_alive=True)


def query_split(node, query):
    return list(
        map(lambda x: x.strip().split("\t"), node.query(query).strip().split("\n"))
    )


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_parts_removal_on_abnormal_exit(start_cluster):
    node.query(
        """
    create table test_parts_removal (key Int) engine=MergeTree order by key;
    insert into test_parts_removal values (1); -- all_1_1_0
    insert into test_parts_removal values (2); -- all_1_2_0
    optimize table test_parts_removal; -- all_2_2_0
    """
    )

    parts = query_split(
        node, "select name, _state from system.parts where table = 'test_parts_removal'"
    )
    assert parts == [
        ["all_1_1_0", "Outdated"],
        ["all_1_2_1", "Active"],
        ["all_2_2_0", "Outdated"],
    ]

    node.restart_clickhouse(kill=True)

    parts = query_split(
        node, "select name, _state from system.parts where table = 'test_parts_removal'"
    )
    assert parts == [
        ["all_1_1_0", "Outdated"],
        ["all_1_2_1", "Active"],
        ["all_2_2_0", "Outdated"],
    ]

    node.query(
        """
    detach table test_parts_removal;
    attach table test_parts_removal;
    """
    )

    parts = query_split(
        node, "select name, _state from system.parts where table = 'test_parts_removal'"
    )
    assert parts == [
        ["all_1_2_1", "Active"],
    ]
