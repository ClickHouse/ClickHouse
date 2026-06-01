import os

import pytest

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)

# Local-storage nodes in a cluster (with Keeper for the distributed DDL queue).
# Used for local persistence, config-handler priority, PROTOCOL scoping and ON CLUSTER.
local1 = cluster.add_instance(
    "local1",
    main_configs=["configs/config.d/handlers_local.xml"],
    user_configs=["configs/users.d/users.xml"],
    with_zookeeper=True,
    stay_alive=True,
)
local2 = cluster.add_instance(
    "local2",
    main_configs=["configs/config.d/handlers_local.xml"],
    user_configs=["configs/users.d/users.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

# Keeper-storage nodes, used to verify that handlers are replicated automatically.
replica1 = cluster.add_instance(
    "replica1",
    main_configs=["configs/config.d/handlers_keeper.xml"],
    user_configs=["configs/users.d/users.xml"],
    with_zookeeper=True,
    stay_alive=True,
)
replica2 = cluster.add_instance(
    "replica2",
    main_configs=["configs/config.d/handlers_keeper.xml"],
    user_configs=["configs/users.d/users.xml"],
    with_zookeeper=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def http_get(node, path, port=8123):
    """GET a URL path from a node, returning the response body (stripped)."""
    res = node.exec_in_container(
        ["bash", "-c", f"curl -s http://127.0.0.1:{port}/{path}"], user="root"
    )
    return res.strip()


def test_local_storage_persists_across_restart(started_cluster):
    local1.query("DROP HANDLER IF EXISTS persist_h")
    local1.query("CREATE HANDLER persist_h URL '/persist' AS SELECT 'persisted' FORMAT TSV")

    assert local1.query("SELECT url FROM system.handlers WHERE name = 'persist_h'").strip() == "/persist"
    assert http_get(local1, "persist") == "persisted"

    local1.restart_clickhouse()

    # The handler survives the restart (loaded from local storage) and is still invokable.
    assert local1.query("SELECT count() FROM system.handlers WHERE name = 'persist_h'").strip() == "1"
    assert http_get(local1, "persist") == "persisted"

    local1.query("DROP HANDLER persist_h")


def test_config_handler_has_priority_over_sql_handler(started_cluster):
    # A SQL-defined handler on the same URL as a configuration-defined handler must be shadowed.
    local1.query("DROP HANDLER IF EXISTS prio_h")
    local1.query("CREATE HANDLER prio_h URL '/config_priority' AS SELECT 'from_sql' FORMAT TSV")

    assert http_get(local1, "config_priority") == "from_config"

    local1.query("DROP HANDLER prio_h")


def test_protocol_scoping(started_cluster):
    local1.query("DROP HANDLER IF EXISTS proto_h")
    local1.query("DROP HANDLER IF EXISTS noproto_h")

    # Active only for the composable protocol named `myproto` (port 8124).
    local1.query("CREATE HANDLER proto_h PROTOCOL myproto URL '/proto_only' AS SELECT 'proto' FORMAT TSV")
    # Active for all http/https protocols (no PROTOCOL clause).
    local1.query("CREATE HANDLER noproto_h URL '/any_proto' AS SELECT 'any' FORMAT TSV")

    # The protocol-scoped handler is served on its protocol's port (8124) ...
    assert http_get(local1, "proto_only", port=8124) == "proto"
    # ... but not on the default http port (8123): no handler matches, so it is a 404 page.
    assert "proto" != http_get(local1, "proto_only", port=8123)

    # The protocol-less handler is served on both ports.
    assert http_get(local1, "any_proto", port=8123) == "any"
    assert http_get(local1, "any_proto", port=8124) == "any"

    local1.query("DROP HANDLER proto_h")
    local1.query("DROP HANDLER noproto_h")


def test_on_cluster(started_cluster):
    for n in (local1, local2):
        n.query("DROP HANDLER IF EXISTS cluster_h")

    local1.query(
        "CREATE HANDLER cluster_h ON CLUSTER lc URL '/cluster' AS SELECT 'clustered' FORMAT TSV"
    )

    # The handler is created on every node of the cluster's local storage and invokable on each.
    for n in (local1, local2):
        assert n.query("SELECT count() FROM system.handlers WHERE name = 'cluster_h'").strip() == "1"
        assert http_get(n, "cluster") == "clustered"

    local1.query("DROP HANDLER cluster_h ON CLUSTER lc")

    for n in (local1, local2):
        assert n.query("SELECT count() FROM system.handlers WHERE name = 'cluster_h'").strip() == "0"


def test_keeper_storage_replication(started_cluster):
    replica1.query("DROP HANDLER IF EXISTS repl_h")

    replica1.query("CREATE HANDLER repl_h URL '/replicated' AS SELECT 'replicated' FORMAT TSV")

    # The handler appears on the other node via the Keeper-backed storage and is invokable there.
    replica2.query_with_retry(
        "SELECT count() FROM system.handlers WHERE name = 'repl_h'",
        check_callback=lambda res: res.strip() == "1",
        retry_count=30,
        sleep_time=1,
    )
    assert http_get(replica2, "replicated") == "replicated"

    replica1.query("DROP HANDLER repl_h")

    # The drop is replicated too.
    replica2.query_with_retry(
        "SELECT count() FROM system.handlers WHERE name = 'repl_h'",
        check_callback=lambda res: res.strip() == "0",
        retry_count=30,
        sleep_time=1,
    )
