import os
import threading

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

    # ALTER only changes the handler's data (not the set of handlers); it must still propagate
    # to the other replica via the Keeper-backed storage.
    replica1.query("ALTER HANDLER repl_h AS SELECT 'altered' FORMAT TSV")
    replica2.query_with_retry(
        "SELECT query FROM system.handlers WHERE name = 'repl_h'",
        check_callback=lambda res: res.strip() == "SELECT 'altered' FORMAT TSV",
        retry_count=30,
        sleep_time=1,
    )
    assert http_get(replica2, "replicated") == "altered"

    replica1.query("DROP HANDLER repl_h")

    # The drop is replicated too.
    replica2.query_with_retry(
        "SELECT count() FROM system.handlers WHERE name = 'repl_h'",
        check_callback=lambda res: res.strip() == "0",
        retry_count=30,
        sleep_time=1,
    )


def test_ignore_on_cluster_for_replicated_storage(started_cluster):
    # With Keeper-backed storage handlers are synchronized automatically, so an ON CLUSTER clause is
    # redundant: fanning it out makes the second host fail with HANDLER_ALREADY_EXISTS. The
    # `ignore_on_cluster_for_replicated_handler_queries` setting makes ON CLUSTER a no-op, so the
    # handler is created once locally and replicated to the other node via Keeper.
    for n in (replica1, replica2):
        n.query("DROP HANDLER IF EXISTS on_cluster_repl_h")

    settings = {"ignore_on_cluster_for_replicated_handler_queries": 1}

    replica1.query(
        "CREATE HANDLER on_cluster_repl_h ON CLUSTER rc URL '/on_cluster_repl' AS SELECT 'ok' FORMAT TSV",
        settings=settings,
    )

    # Created exactly once on the initiator and replicated to the other node (not double-created).
    for n in (replica1, replica2):
        n.query_with_retry(
            "SELECT count() FROM system.handlers WHERE name = 'on_cluster_repl_h'",
            check_callback=lambda res: res.strip() == "1",
            retry_count=30,
            sleep_time=1,
        )
        assert http_get(n, "on_cluster_repl") == "ok"

    # ALTER and DROP with ON CLUSTER are ignored the same way.
    replica1.query(
        "ALTER HANDLER on_cluster_repl_h ON CLUSTER rc AS SELECT 'ok2' FORMAT TSV",
        settings=settings,
    )
    replica2.query_with_retry(
        "SELECT query FROM system.handlers WHERE name = 'on_cluster_repl_h'",
        check_callback=lambda res: res.strip() == "SELECT 'ok2' FORMAT TSV",
        retry_count=30,
        sleep_time=1,
    )

    replica1.query(
        "DROP HANDLER on_cluster_repl_h ON CLUSTER rc",
        settings=settings,
    )
    for n in (replica1, replica2):
        n.query_with_retry(
            "SELECT count() FROM system.handlers WHERE name = 'on_cluster_repl_h'",
            check_callback=lambda res: res.strip() == "0",
            retry_count=30,
            sleep_time=1,
        )


def test_concurrent_overlapping_create_is_serialized(started_cluster):
    # Two replicas concurrently create handlers with *different* names but *overlapping* URLs (the same
    # exact URL, default GET, no protocol). The exact/prefix ambiguity guarantee must hold across replicas:
    # the two overlapping handlers must never both end up committed. With Keeper-backed storage the
    # ambiguity check is serialized with the write via optimistic concurrency on the root version, so
    # exactly one create wins and the other is rejected with AMBIGUOUS_HANDLER.
    def cleanup():
        for n in (replica1, replica2):
            n.query("DROP HANDLER IF EXISTS race_a")
            n.query("DROP HANDLER IF EXISTS race_b")
        # Wait for the drops to converge on both replicas before the next round.
        for n in (replica1, replica2):
            n.query_with_retry(
                "SELECT count() FROM system.handlers WHERE url = '/race'",
                check_callback=lambda res: res.strip() == "0",
                retry_count=30,
                sleep_time=0.5,
            )

    cleanup()

    for _ in range(15):
        errors = []
        errors_lock = threading.Lock()

        def create(node, name):
            try:
                node.query(f"CREATE HANDLER {name} URL '/race' AS SELECT 1")
            except Exception as e:  # noqa: BLE001
                with errors_lock:
                    errors.append(str(e))

        t1 = threading.Thread(target=create, args=(replica1, "race_a"))
        t2 = threading.Thread(target=create, args=(replica2, "race_b"))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # Exactly one create succeeded; the loser was rejected as ambiguous (never a silent double-commit).
        assert len(errors) == 1, f"expected exactly one failure, got: {errors}"
        assert "AMBIGUOUS_HANDLER" in errors[0], errors[0]

        # After convergence both replicas agree that exactly one handler matches the contested URL.
        for n in (replica1, replica2):
            n.query_with_retry(
                "SELECT count() FROM system.handlers WHERE url = '/race'",
                check_callback=lambda res: res.strip() == "1",
                retry_count=30,
                sleep_time=0.5,
            )

        cleanup()


def test_drop_resolves_against_persistent_state(started_cluster):
    # On Keeper-backed storage the existence of a handler for a DROP must be resolved against the persistent
    # store, not the local in-memory snapshot (which may be stale on a replica that has not yet reloaded a
    # handler created elsewhere). A plain DROP of a handler that exists nowhere is an error; IF EXISTS is a
    # no-op; and a DROP issued on the replica that did not create the handler still succeeds.
    for n in (replica1, replica2):
        n.query("DROP HANDLER IF EXISTS drop_persist_h")
        n.query_with_retry(
            "SELECT count() FROM system.handlers WHERE name = 'drop_persist_h'",
            check_callback=lambda res: res.strip() == "0",
            retry_count=30,
            sleep_time=0.5,
        )

    # Plain DROP of a handler that exists in neither the cache nor Keeper is an error.
    assert "HANDLER_DOESNT_EXIST" in replica2.query_and_get_error(
        "DROP HANDLER drop_persist_h"
    )
    # IF EXISTS of a missing handler is a silent no-op.
    replica2.query("DROP HANDLER IF EXISTS drop_persist_h")

    # Create on one replica, wait for it to converge on the other, then drop it from the replica that did
    # not create it: the drop is resolved against the persistent store, where the handler exists.
    replica1.query(
        "CREATE HANDLER drop_persist_h URL '/drop_persist' AS SELECT 1 FORMAT TSV"
    )
    replica2.query_with_retry(
        "SELECT count() FROM system.handlers WHERE name = 'drop_persist_h'",
        check_callback=lambda res: res.strip() == "1",
        retry_count=30,
        sleep_time=0.5,
    )
    replica2.query("DROP HANDLER drop_persist_h")

    # The drop is replicated back to the creator.
    for n in (replica1, replica2):
        n.query_with_retry(
            "SELECT count() FROM system.handlers WHERE name = 'drop_persist_h'",
            check_callback=lambda res: res.strip() == "0",
            retry_count=30,
            sleep_time=0.5,
        )
