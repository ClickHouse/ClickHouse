import base64
import inspect
from contextlib import nullcontext as does_not_raise
from os import path as p

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.keeper_utils import (
    get_active_zk_connections,
    replace_zookeeper_config,
    reset_zookeeper_config,
)
from helpers.test_tools import TSV, assert_eq_with_retry

default_zk_config = p.join(p.dirname(p.realpath(__file__)), "configs/zookeeper.xml")
cluster = ClickHouseCluster(__file__, zookeeper_config_path="configs/zookeeper.xml")

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    stay_alive=True,
    with_remote_database_disk=False,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    stay_alive=True,
    with_remote_database_disk=False,  # Disable `with_remote_database_disk` as test_start_without_zookeeper stops keeper before starting.
)

all_nodes = [node1, node2]
wasm_dir = p.join(
    p.dirname(p.realpath(__file__)), "..", "..", "queries", "0_stateless", "wasm"
)


def read_wasm_base64(file_name):
    with open(p.join(wasm_dir, file_name), "rb") as wasm_file:
        return base64.b64encode(wasm_file.read()).decode()


def insert_wasm_module(node, module_name, file_name):
    node.query(f"DELETE FROM system.webassembly_modules WHERE name = '{module_name}'")
    node.query(
        "INSERT INTO system.webassembly_modules (name, code) "
        f"SELECT '{module_name}', base64Decode('{read_wasm_base64(file_name)}')"
    )


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


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
    node1.query(
        "CREATE FUNCTION f3 AS () -> (SELECT sum(s) FROM (SELECT 1 as s UNION ALL SELECT 1 as s))"
    )

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

    assert node1.query("SELECT f3()") == "2\n"
    assert node2.query("SELECT f3()") == "2\n"

    node1.query("DROP FUNCTION f2")
    node1.query("DROP FUNCTION f3")
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


def test_replicated_wasm_udf_registry_refresh():
    if node1.is_built_with_memory_sanitizer() or node2.is_built_with_memory_sanitizer():
        pytest.skip("Wasmtime is disabled in MSAN builds")

    function_name = "replicated_wasm_udf"
    identity_module = "replicated_wasm_identity"
    replacement_module = "replicated_wasm_replacement"

    for node in all_nodes:
        node.query("DROP VIEW IF EXISTS replicated_wasm_mv")
        node.query("DROP TABLE IF EXISTS replicated_wasm_mv_src")
        node.query("DROP TABLE IF EXISTS replicated_wasm_mv_dst")

    node1.query(f"DROP FUNCTION IF EXISTS {function_name}")
    assert_eq_with_retry(
        node2,
        f"SELECT name FROM system.functions WHERE name = '{function_name}'",
        "",
    )

    try:
        for node in all_nodes:
            insert_wasm_module(node, identity_module, "identity_int.wasm")
            insert_wasm_module(node, replacement_module, "faulty.wasm")

        node1.query(
            f"""
            CREATE FUNCTION {function_name}
            LANGUAGE WASM ABI ROW_DIRECT
            FROM '{identity_module}' :: 'identity_raw'
            ARGUMENTS (value UInt32)
            RETURNS UInt32
            """
        )

        assert_eq_with_retry(
            node2,
            f"SELECT {function_name}(6 :: UInt32)",
            "6\n",
        )

        node2.query(
            "CREATE TABLE replicated_wasm_mv_src (value UInt32) ENGINE = Memory"
        )
        node2.query(
            "CREATE TABLE replicated_wasm_mv_dst (value UInt32) ENGINE = Memory"
        )
        node2.query(
            f"""
            CREATE MATERIALIZED VIEW replicated_wasm_mv TO replicated_wasm_mv_dst AS
            SELECT {function_name}(value) AS value
            FROM replicated_wasm_mv_src
            """
        )
        node2.query("INSERT INTO replicated_wasm_mv_src VALUES (6)")
        assert node2.query("SELECT value FROM replicated_wasm_mv_dst") == "6\n"
        node2.query("DROP VIEW replicated_wasm_mv")
        node2.query("DROP TABLE replicated_wasm_mv_src")
        node2.query("DROP TABLE replicated_wasm_mv_dst")

        node1.query(
            f"""
            CREATE OR REPLACE FUNCTION {function_name}
            LANGUAGE WASM ABI ROW_DIRECT
            FROM '{replacement_module}' :: 'fib'
            ARGUMENTS (value UInt32)
            RETURNS UInt32
            """
        )

        assert_eq_with_retry(
            node2,
            f"SELECT {function_name}(6 :: UInt32)",
            "13\n",
        )

        node2.query(
            "CREATE TABLE replicated_wasm_mv_src (value UInt32) ENGINE = Memory"
        )
        node2.query(
            "CREATE TABLE replicated_wasm_mv_dst (value UInt32) ENGINE = Memory"
        )
        node2.query(
            f"""
            CREATE MATERIALIZED VIEW replicated_wasm_mv TO replicated_wasm_mv_dst AS
            SELECT {function_name}(value) AS value
            FROM replicated_wasm_mv_src
            """
        )
        node2.query("INSERT INTO replicated_wasm_mv_src VALUES (6)")
        assert node2.query("SELECT value FROM replicated_wasm_mv_dst") == "13\n"

        node1.query(f"DROP FUNCTION {function_name}")
        assert_eq_with_retry(
            node2,
            f"SELECT name FROM system.functions WHERE name = '{function_name}'",
            "",
        )
    finally:
        node1.query(f"DROP FUNCTION IF EXISTS {function_name}")
        for node in all_nodes:
            node.query("DROP VIEW IF EXISTS replicated_wasm_mv")
            node.query("DROP TABLE IF EXISTS replicated_wasm_mv_src")
            node.query("DROP TABLE IF EXISTS replicated_wasm_mv_dst")
            node.query(
                f"DELETE FROM system.webassembly_modules WHERE name = '{identity_module}'"
            )
            node.query(
                f"DELETE FROM system.webassembly_modules WHERE name = '{replacement_module}'"
            )


# UserDefinedSQLObjectsLoaderFromZooKeeper must be able to continue working after reloading ZooKeeper.
def test_reload_zookeeper():
    node1.query("CREATE FUNCTION f1 AS (x, y) -> x + y")
    assert_eq_with_retry(
        node2, "SELECT name FROM system.functions WHERE name ='f1'", "f1\n"
    )

    # remove zoo2, zoo3 from configs
    replace_zookeeper_config(
        (node1, node2),
        inspect.cleandoc(
            """
            <clickhouse>
                <zookeeper>
                    <node index="1">
                        <host>zoo1</host>
                        <port>2181</port>
                    </node>
                    <session_timeout_ms>2000</session_timeout_ms>
                </zookeeper>
            </clickhouse>
            """
        ),
    )

    # config reloads, but can still work
    node1.query(
        "CREATE FUNCTION f2 AS () -> (SELECT sum(s) FROM (SELECT 1 as s UNION ALL SELECT 1 as s))"
    )
    assert_eq_with_retry(
        node2,
        "SELECT name FROM system.functions WHERE name IN ['f1', 'f2'] ORDER BY name",
        TSV(["f1", "f2"]),
    )

    # stop all zookeepers, user-defined functions will be readonly
    cluster.stop_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])
    assert node2.query(
        "SELECT name FROM system.functions WHERE name IN ['f1', 'f2'] ORDER BY name"
    ) == TSV(["f1", "f2"])
    assert "ZooKeeper" in node1.query_and_get_error(
        "CREATE FUNCTION f3 AS (x, y) -> x * y"
    )

    # start zoo2, zoo3, user-defined functions will be readonly too, because it only connect to zoo1
    cluster.start_zookeeper_nodes(["zoo2", "zoo3"])
    cluster.wait_zookeeper_nodes_to_start(["zoo2", "zoo3"])
    assert node2.query(
        "SELECT name FROM system.functions WHERE name IN ['f1', 'f2', 'f3'] ORDER BY name"
    ) == TSV(["f1", "f2"])
    assert "ZooKeeper" in node1.query_and_get_error(
        "CREATE FUNCTION f3 AS (x, y) -> x * y"
    )

    # set config to zoo2, server will be normal
    replace_zookeeper_config(
        (node1, node2),
        inspect.cleandoc(
            """
            <clickhouse>
                <zookeeper>
                    <node index="1">
                        <host>zoo2</host>
                        <port>2181</port>
                    </node>
                    <session_timeout_ms>2000</session_timeout_ms>
                </zookeeper>
            </clickhouse>
            """
        ),
    )

    active_zk_connections = get_active_zk_connections(node1)
    assert (
        len(active_zk_connections) == 1
    ), "Total connections to ZooKeeper not equal to 1, {}".format(active_zk_connections)

    node1.query("CREATE FUNCTION f3 AS (x, y) -> x / y")
    assert_eq_with_retry(
        node2,
        "SELECT name FROM system.functions WHERE name IN ['f1', 'f2', 'f3'] ORDER BY name",
        TSV(["f1", "f2", "f3"]),
    )

    assert node2.query("SELECT f1(12, 3), f2(), f3(12, 3)") == TSV([[15, 2, 4]])

    active_zk_connections = get_active_zk_connections(node1)
    assert (
        len(active_zk_connections) == 1
    ), "Total connections to ZooKeeper not equal to 1, {}".format(active_zk_connections)

    node1.query("DROP FUNCTION f1")
    node1.query("DROP FUNCTION f2")
    node1.query("DROP FUNCTION f3")

    # switch to the original version of zookeeper config
    cluster.start_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])
    reset_zookeeper_config((node1, node2), default_zk_config)


# Start without ZooKeeper must be possible, user-defined functions will be loaded after connecting to ZooKeeper.
def test_start_without_zookeeper():
    node2.stop_clickhouse()

    node1.query("CREATE FUNCTION f1 AS (x, y) -> x + y")

    cluster.stop_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])
    node2.start_clickhouse()

    assert (
        node2.query("SELECT create_query FROM system.functions WHERE name='f1'") == ""
    )

    cluster.start_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])
    cluster.wait_zookeeper_nodes_to_start(["zoo1", "zoo2", "zoo3"])

    assert_eq_with_retry(
        node2,
        "SELECT create_query FROM system.functions WHERE name='f1'",
        "CREATE FUNCTION f1 AS (x, y) -> (x + y)\n",
    )
    node1.query("DROP FUNCTION f1")


def test_server_restart():
    node1.query(
        "CREATE FUNCTION f1 AS () -> (SELECT sum(s) FROM (SELECT 1 as s UNION ALL SELECT 1 as s))"
    )
    assert node1.query("SELECT f1()") == "2\n"
    node1.restart_clickhouse()
    assert node1.query("SELECT f1()") == "2\n"
    node1.query("DROP FUNCTION f1")
