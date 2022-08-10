import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.d/remote_servers.xml"],
    user_configs=["configs/users.d/allow_introspection_functions.xml"],
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.d/remote_servers.xml"],
    user_configs=["configs/users.d/allow_introspection_functions.xml"],
)
distributed = cluster.add_instance(
    "distributed",
    main_configs=["configs/config.d/remote_servers.xml"],
    user_configs=["configs/users.d/allow_introspection_functions.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        for node in [node1, node2]:
            node.query("CREATE USER shard")
            node.query("GRANT ALL ON *.* TO shard")

        distributed.query("CREATE ROLE admin")
        distributed.query("GRANT ALL ON *.* TO admin")
        distributed.query(
            "CREATE TABLE shard_settings (name String, value String) ENGINE = Distributed(test_cluster, system, settings);"
        )

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def restart_distributed():
    # Magic: Distributed table tries to keep connections to shards open, and after changing shards' default settings
    # we need to reset connections to force the shards to reset sessions and therefore to reset current settings
    # to their new defaults.
    distributed.restart_clickhouse()


def test_select_clamps_settings():
    for node in [node1, node2]:
        node.query(
            "CREATE TABLE sometable_select (date Date, id UInt32, value Int32) ENGINE = MergeTree() ORDER BY id;"
        )
        node.query("INSERT INTO sometable_select VALUES (toDate('2010-01-10'), 1, 1)")

    distributed.query(
        "CREATE TABLE proxy_select (date Date, id UInt32, value Int32) ENGINE = Distributed(test_cluster, default, sometable_select, toUInt64(date));"
    )

    distributed.query(
        "CREATE USER normal DEFAULT ROLE admin SETTINGS max_memory_usage = 80000000"
    )
    distributed.query(
        "CREATE USER wasteful DEFAULT ROLE admin SETTINGS max_memory_usage = 2000000000"
    )
    distributed.query("CREATE USER readonly DEFAULT ROLE admin SETTINGS readonly = 1")
    node1.query(
        "ALTER USER shard SETTINGS max_memory_usage = 50000000 MIN 11111111 MAX 99999999"
    )
    node2.query("ALTER USER shard SETTINGS readonly = 1")

    # Check that shards doesn't throw exceptions on constraints violation
    query = "SELECT COUNT() FROM proxy_select"
    assert distributed.query(query) == "2\n"
    assert distributed.query(query, user="normal") == "2\n"
    assert distributed.query(query, user="wasteful") == "2\n"
    assert distributed.query(query, user="readonly") == "2\n"

    assert (
        distributed.query(query, settings={"max_memory_usage": 40000000, "readonly": 2})
        == "2\n"
    )
    assert (
        distributed.query(
            query, settings={"max_memory_usage": 3000000000, "readonly": 2}
        )
        == "2\n"
    )

    query = "SELECT COUNT() FROM remote('node{1,2}', 'default', 'sometable_select')"
    assert distributed.query(query) == "2\n"
    assert distributed.query(query, user="normal") == "2\n"
    assert distributed.query(query, user="wasteful") == "2\n"

    # Check that shards clamp passed settings.
    query = "SELECT hostName() as host, name, value FROM shard_settings WHERE name = 'max_memory_usage' OR name = 'readonly' ORDER BY host, name, value"
    assert (
        distributed.query(query) == "node1\tmax_memory_usage\t99999999\n"
        "node1\treadonly\t0\n"
        "node2\tmax_memory_usage\t10000000000\n"
        "node2\treadonly\t1\n"
    )
    assert (
        distributed.query(query, user="normal") == "node1\tmax_memory_usage\t80000000\n"
        "node1\treadonly\t0\n"
        "node2\tmax_memory_usage\t10000000000\n"
        "node2\treadonly\t1\n"
    )
    assert (
        distributed.query(query, user="wasteful")
        == "node1\tmax_memory_usage\t99999999\n"
        "node1\treadonly\t0\n"
        "node2\tmax_memory_usage\t10000000000\n"
        "node2\treadonly\t1\n"
    )
    assert (
        distributed.query(query, user="readonly")
        == "node1\tmax_memory_usage\t99999999\n"
        "node1\treadonly\t1\n"
        "node2\tmax_memory_usage\t10000000000\n"
        "node2\treadonly\t1\n"
    )

    assert (
        distributed.query(query, settings={"max_memory_usage": 1})
        == "node1\tmax_memory_usage\t11111111\n"
        "node1\treadonly\t0\n"
        "node2\tmax_memory_usage\t10000000000\n"
        "node2\treadonly\t1\n"
    )
    assert (
        distributed.query(query, settings={"max_memory_usage": 40000000, "readonly": 2})
        == "node1\tmax_memory_usage\t40000000\n"
        "node1\treadonly\t2\n"
        "node2\tmax_memory_usage\t10000000000\n"
        "node2\treadonly\t1\n"
    )
    assert (
        distributed.query(
            query, settings={"max_memory_usage": 3000000000, "readonly": 2}
        )
        == "node1\tmax_memory_usage\t99999999\n"
        "node1\treadonly\t2\n"
        "node2\tmax_memory_usage\t10000000000\n"
        "node2\treadonly\t1\n"
    )


def test_insert_clamps_settings():
    for node in [node1, node2]:
        node.query(
            "CREATE TABLE sometable_insert (date Date, id UInt32, value Int32) ENGINE = MergeTree() ORDER BY id;"
        )
        node.query("INSERT INTO sometable_insert VALUES (toDate('2010-01-10'), 1, 1)")

    distributed.query(
        "CREATE TABLE proxy_insert (date Date, id UInt32, value Int32) ENGINE = Distributed(test_cluster, default, sometable_insert, toUInt64(date));"
    )

    node1.query(
        "ALTER USER shard SETTINGS max_memory_usage = 50000000 MIN 11111111 MAX 99999999"
    )
    node2.query(
        "ALTER USER shard SETTINGS max_memory_usage = 50000000 MIN 11111111 MAX 99999999"
    )

    distributed.query("INSERT INTO proxy_insert VALUES (toDate('2020-02-20'), 2, 2)")
    distributed.query(
        "INSERT INTO proxy_insert VALUES (toDate('2020-02-21'), 2, 2)",
        settings={"max_memory_usage": 5000000},
    )
    distributed.query("SYSTEM FLUSH DISTRIBUTED proxy_insert")
    assert_eq_with_retry(distributed, "SELECT COUNT() FROM proxy_insert", "4")
