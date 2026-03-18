"""
Test cases:

--- execute on the first node
create named collection foobar as a=1, b=2;
create named collection if not exists foobar on cluster '{cluster}' as a=1, b=2, c=3;
create named collection collection_present_on_first_node as a=1, b=2, s='string', x=0, y=-1;

--- execute on any other node
alter named collection foobar on cluster '{cluster}' set a=2, c=3;
alter named collection foobar on cluster '{cluster}' delete b;
alter named collection foobar on cluster '{cluster}' set a=3 delete c;
alter named collection if exists collection_absent_ewerywhere on cluster '{cluster}' delete b;
alter named collection if exists collection_present_on_first_node on cluster '{cluster}' delete b;

--- execute on every node
select * from system.named_collections;

--- execute on any node
drop named collection foobar on cluster '{cluster}';
drop named collection if exists collection_absent_ewerywhere on cluster '{cluster}';
drop named collection if exists collection_present_on_first_node on cluster '{cluster}';

--- execute on every node
select * from system.named_collections;
"""

import logging
from functools import partial
from json import dumps, loads

import pytest

from helpers.cluster import ClickHouseCluster

dumps = partial(dumps, ensure_ascii=False)

NODE01, NODE02, NODE03 = "clickhouse1", "clickhouse2", "clickhouse3"

CHECK_STRING_VALUE = "Some ~`$tr!ng-_+=123@#%^&&()|?[]{}<🤡>.,\t\n:;"

STMT_CREATE = "CREATE NAMED COLLECTION"
STMT_ALTER = "ALTER NAMED COLLECTION"
STMT_DROP = "DROP NAMED COLLECTION"

SYSTEM_TABLE = "system.named_collections"


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        common_kwargs = dict(
            main_configs=[
                "configs/config.d/cluster.xml",
            ],
            user_configs=[
                "configs/users.d/default.xml",
            ],
            with_zookeeper=True,
            stay_alive=True,
        )
        for name in [NODE01, NODE02, NODE03]:
            cluster.add_instance(name, **common_kwargs)

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def test_create_alter_drop_on_cluster(cluster):
    """
    Executes the set of queries and checks the final named collections state.
    """
    q_count_collections = f"select count() from {SYSTEM_TABLE}"

    def check_collections_empty():
        for name, node in list(cluster.instances.items()):
            assert (
                "0" == node.query(q_count_collections).strip()
            ), f"{SYSTEM_TABLE} is not empty on {name}"

    foobar_final_state = {"name": "foobar", "collection": {"a": "3"}}
    collection_present_on_first_node_final_state = {
        "name": "collection_present_on_first_node",
        "collection": {"a": "1", "s": CHECK_STRING_VALUE, "x": "0", "y": "-1"},
    }
    expected_state = {
        NODE01: [foobar_final_state, collection_present_on_first_node_final_state],
        NODE02: [foobar_final_state],
        NODE03: [foobar_final_state],
    }

    q_get_collections = f"select * from {SYSTEM_TABLE} order by name desc format JSON"

    def check_state():
        for name, node in list(cluster.instances.items()):
            result = loads(node.query(q_get_collections))["data"]
            logging.debug("%s ?= %s", dumps(result), dumps(expected_state[name]))
            assert (
                expected_state[name] == result
            ), f"invalid {SYSTEM_TABLE} content on {name}: {result}"

    check_collections_empty()

    # create executed on the first node
    node = cluster.instances[NODE01]
    node.query(f"{STMT_CREATE} foobar AS a=1, b=2")
    node.query(
        f"{STMT_CREATE} IF NOT EXISTS foobar ON CLUSTER 'cluster' AS a=1, b=2, c=3"
    )
    node.query(
        f"{STMT_CREATE} collection_present_on_first_node AS a=1, b=2, s='{CHECK_STRING_VALUE}', x=0, y=-1"
    )

    # alter executed on the second node
    node = cluster.instances[NODE02]
    node.query(f"{STMT_ALTER} foobar ON CLUSTER 'cluster' SET a=2, c=3")
    node.query(f"{STMT_ALTER} foobar ON CLUSTER 'cluster' DELETE b")
    node.query(f"{STMT_ALTER} foobar ON CLUSTER 'cluster' SET a=3 DELETE c")
    node.query(
        f"{STMT_ALTER} IF EXISTS collection_absent_ewerywhere ON CLUSTER 'cluster' DELETE b"
    )
    node.query(
        f"{STMT_ALTER} IF EXISTS collection_present_on_first_node ON CLUSTER 'cluster' DELETE b"
    )

    check_state()
    for node in list(cluster.instances.values()):
        node.restart_clickhouse()
    check_state()

    # drop executed on the third node
    node = cluster.instances[NODE03]
    node.query(f"{STMT_DROP} foobar ON CLUSTER 'cluster'")
    node.query(
        f"{STMT_DROP} IF EXISTS collection_absent_ewerywhere ON CLUSTER 'cluster'"
    )
    node.query(
        f"{STMT_DROP} IF EXISTS collection_present_on_first_node ON CLUSTER 'cluster'"
    )

    check_collections_empty()
    for node in list(cluster.instances.values()):
        node.restart_clickhouse()
    check_collections_empty()
