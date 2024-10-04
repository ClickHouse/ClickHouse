import inspect
import os.path
import time
from contextlib import nullcontext as does_not_raise

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV, assert_eq_with_retry

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__, zookeeper_config_path="configs/zookeeper.xml")

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


def wait_zookeeper_node_to_start(zk_nodes, timeout=60):
    start = time.time()
    while time.time() - start < timeout:
        try:
            for instance in zk_nodes:
                conn = cluster.get_kazoo_client(instance)
                conn.get_children("/")
            print("All instances of ZooKeeper started")
            return
        except Exception as ex:
            print(("Can't connect to ZooKeeper " + str(ex)))
            time.sleep(0.5)


def replace_zookeeper_config(new_config):
    node1.replace_config("/etc/clickhouse-server/conf.d/zookeeper.xml", new_config)
    node2.replace_config("/etc/clickhouse-server/conf.d/zookeeper.xml", new_config)
    node1.query("SYSTEM RELOAD CONFIG")
    node2.query("SYSTEM RELOAD CONFIG")


def revert_zookeeper_config():
    with open(os.path.join(SCRIPT_DIR, "configs/zookeeper.xml"), "r") as f:
        replace_zookeeper_config(f.read())


def get_active_zk_connections():
    return str(
        node1.exec_in_container(
            [
                "bash",
                "-c",
                "lsof -a -i4 -i6 -itcp -w | grep 2181 | grep ESTABLISHED | wc -l",
            ],
            privileged=True,
            user="root",
        )
    ).strip()


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


# UserDefinedSQLObjectsLoaderFromZooKeeper must be able to continue working after reloading ZooKeeper.
def test_reload_zookeeper():
    node1.query("CREATE FUNCTION f1 AS (x, y) -> x + y")
    assert_eq_with_retry(
        node2, "SELECT name FROM system.functions WHERE name ='f1'", "f1\n"
    )

    # remove zoo2, zoo3 from configs
    replace_zookeeper_config(
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
        )
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
    wait_zookeeper_node_to_start(["zoo2", "zoo3"])
    assert node2.query(
        "SELECT name FROM system.functions WHERE name IN ['f1', 'f2', 'f3'] ORDER BY name"
    ) == TSV(["f1", "f2"])
    assert "ZooKeeper" in node1.query_and_get_error(
        "CREATE FUNCTION f3 AS (x, y) -> x * y"
    )

    # set config to zoo2, server will be normal
    replace_zookeeper_config(
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
        )
    )

    active_zk_connections = get_active_zk_connections()
    assert (
        active_zk_connections == "1"
    ), "Total connections to ZooKeeper not equal to 1, {}".format(active_zk_connections)

    node1.query("CREATE FUNCTION f3 AS (x, y) -> x / y")
    assert_eq_with_retry(
        node2,
        "SELECT name FROM system.functions WHERE name IN ['f1', 'f2', 'f3'] ORDER BY name",
        TSV(["f1", "f2", "f3"]),
    )

    assert node2.query("SELECT f1(12, 3), f2(), f3(12, 3)") == TSV([[15, 2, 4]])

    active_zk_connections = get_active_zk_connections()
    assert (
        active_zk_connections == "1"
    ), "Total connections to ZooKeeper not equal to 1, {}".format(active_zk_connections)

    node1.query("DROP FUNCTION f1")
    node1.query("DROP FUNCTION f2")
    node1.query("DROP FUNCTION f3")

    # switch to the original version of zookeeper config
    cluster.start_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])
    revert_zookeeper_config()


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
    wait_zookeeper_node_to_start(["zoo1", "zoo2", "zoo3"])

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
