import inspect
from contextlib import nullcontext as does_not_raise

import pytest
import time
import os.path
import logging

from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException
from helpers.test_tools import assert_eq_with_retry, TSV

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

all_nodes = [node1, node2]


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def replace_fdbkeeper_cluster(new_fdb_cluster):
    logging.debug(f"Use new fdb cluster: {new_fdb_cluster}")
    for node in [node1, node2]:
        node.replace_config("/tmp/fdb.cluster", new_fdb_cluster)
        node.replace_in_config(
            "/etc/clickhouse-server/conf.d/fdb_config.xml",
            "\\/etc\\/foundationdb\\/fdb.cluster",
            "\\/tmp\\/fdb.cluster",
        )
        node.query("SYSTEM RELOAD CONFIG")


# UserDefinedSQLObjectsLoaderFromZooKeeper must be able to continue working after reloading ZooKeeper.
def test_reload_fdbkeeper():
    node1.query("CREATE FUNCTION f1 AS (x, y) -> x + y")
    assert_eq_with_retry(
        node2, "SELECT name FROM system.functions WHERE name ='f1'", "f1\n"
    )

    node1.query("CREATE FUNCTION f2 AS (x, y) -> x - y")

    ## stop fdb, users will be readonly
    cluster.stop_fdb()
    assert node2.query(
        "SELECT name FROM system.functions WHERE name IN ['f1', 'f2'] ORDER BY name"
    ) == TSV(["f1", "f2"])
    assert "ZooKeeper" in node1.query_and_get_error(
        "CREATE FUNCTION f3 AS (x, y) -> x * y"
    )

    ## switch to fdb2, server will be normal
    cluster.switch_to_fdb2()
    replace_fdbkeeper_cluster(cluster.get_fdb2_cluster())
    logging.info(cluster.get_instance_ip(cluster.foundationdb_host))
    assert_eq_with_retry(
        node2,
        "SELECT name FROM system.functions WHERE name IN ['f1', 'f2', 'f3'] ORDER BY name",
        TSV(["f1", "f2"]),
    )


# Start without ZooKeeper must be possible, user-defined functions will be loaded after connecting to ZooKeeper.
def test_start_without_fdbkeeper():
    node2.stop_clickhouse()

    node1.query("CREATE FUNCTION f1 AS (x, y) -> x + y")

    cluster.stop_fdb()
    node2.start_clickhouse()

    assert (
        node2.query("SELECT create_query FROM system.functions WHERE name='f1'") == ""
    )

    cluster.start_fdb()

    assert_eq_with_retry(
        node2,
        "SELECT create_query FROM system.functions WHERE name='f1'",
        "CREATE FUNCTION f1 AS (x, y) -> (x + y)\n",
    )
    node1.query("DROP FUNCTION f1")
