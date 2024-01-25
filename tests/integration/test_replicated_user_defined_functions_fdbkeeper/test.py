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


def test_create_and_drop():
    node1.query("CREATE FUNCTION f1 AS (x, y) -> x + y")
    assert node1.query("SELECT f1(12, 3)") == "15\n"
    node1.query("DROP FUNCTION f1")


@pytest.mark.parametrize(
    "ignore, expected_raise",
    [("true", does_not_raise()), ("false", pytest.raises(QueryRuntimeException))],
)
def test_create_and_drop_udf_on_cluster(ignore, expected_raise):
    node1.replace_config(
        "/etc/clickhouse-server/users.d/users.xml",
        inspect.cleandoc(
            f"""
            <clickhouse>
                <profiles>
                    <default>
                        <ignore_on_cluster_for_replicated_udf_queries>{ignore}</ignore_on_cluster_for_replicated_udf_queries>
                    </default>
                </profiles>
            </clickhouse>
            """
        ),
    )
    node1.query("SYSTEM RELOAD CONFIG")

    with expected_raise:
        node1.query("CREATE FUNCTION f1 ON CLUSTER default AS (x, y) -> x + y")
        assert node1.query("SELECT f1(12, 3)") == "15\n"
        node1.query("DROP FUNCTION f1 ON CLUSTER default")


def test_create_and_replace():
    node1.query("CREATE FUNCTION f1 AS (x, y) -> x + y")
    assert node1.query("SELECT f1(12, 3)") == "15\n"

    expected_error = "User-defined object 'f1' already exists"
    assert expected_error in node1.query_and_get_error(
        "CREATE FUNCTION f1 AS (x, y) -> x + 2 * y"
    )

    node1.query("CREATE FUNCTION IF NOT EXISTS f1 AS (x, y) -> x + 3 * y")
    assert node1.query("SELECT f1(12, 3)") == "15\n"

    node1.query("CREATE OR REPLACE FUNCTION f1 AS (x, y) -> x + 4 * y")
    assert node1.query("SELECT f1(12, 3)") == "24\n"

    node1.query("DROP FUNCTION f1")


def test_drop_if_exists():
    node1.query("CREATE FUNCTION f1 AS (x, y) -> x + y")
    node1.query("DROP FUNCTION IF EXISTS f1")
    node1.query("DROP FUNCTION IF EXISTS f1")

    expected_error = "User-defined object 'f1' doesn't exist"
    assert expected_error in node1.query_and_get_error("DROP FUNCTION f1")


def test_replication():
    node1.query("CREATE FUNCTION f2 AS (x, y) -> x - y")

    assert (
        node1.query("SELECT create_query FROM system.functions WHERE name='f2'")
        == "CREATE FUNCTION f2 AS (x, y) -> (x - y)\n"
    )
    assert_eq_with_retry(
        node2,
        "SELECT create_query FROM system.functions WHERE name='f2'",
        "CREATE FUNCTION f2 AS (x, y) -> (x - y)\n",
    )
    assert node1.query("SELECT f2(12,3)") == "9\n"
    assert node2.query("SELECT f2(12,3)") == "9\n"

    node1.query("DROP FUNCTION f2")
    assert (
        node1.query("SELECT create_query FROM system.functions WHERE name='f2'") == ""
    )
    assert_eq_with_retry(
        node2, "SELECT create_query FROM system.functions WHERE name='f2'", ""
    )


def test_replication_replace_by_another_node_after_creation():
    node1.query("CREATE FUNCTION f2 AS (x, y) -> x - y")

    assert_eq_with_retry(
        node2,
        "SELECT create_query FROM system.functions WHERE name='f2'",
        "CREATE FUNCTION f2 AS (x, y) -> (x - y)\n",
    )

    node2.query("CREATE OR REPLACE FUNCTION f2 AS (x, y) -> x + y")

    assert_eq_with_retry(
        node1,
        "SELECT create_query FROM system.functions WHERE name='f2'",
        "CREATE FUNCTION f2 AS (x, y) -> (x + y)\n",
    )

    node1.query("DROP FUNCTION f2")
    assert_eq_with_retry(
        node1, "SELECT create_query FROM system.functions WHERE name='f2'", ""
    )
    assert_eq_with_retry(
        node2, "SELECT create_query FROM system.functions WHERE name='f2'", ""
    )


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
