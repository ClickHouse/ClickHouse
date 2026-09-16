import time

import pytest

from helpers.cluster import ClickHouseCluster

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

VICTIM_QUERY = (
    "SELECT sleep(0.1) FROM numbers(100000) SETTINGS max_block_size = 1, max_rows_to_read = 0"
)

# Every wait below is bounded and then asserted, so a regression fails instead of hanging.
POLL_ATTEMPTS = 60
POLL_INTERVAL = 0.5


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="function", autouse=True)
def users():
    # Deliberately no SELECT on system.processes and no KILL QUERY: doing without those is the
    # point of the by-id self kill.
    node1.query("CREATE USER u1, u2 IDENTIFIED WITH no_password")
    node1.query("GRANT SELECT ON system.numbers TO u1, u2")
    node1.query("GRANT CLUSTER ON *.* TO u1, u2")
    for user in ["u1", "u2"]:
        wait_for(
            lambda: node2.query(
                f"SELECT count() FROM system.users WHERE name = '{user}'"
            ).strip()
            == "1",
            f"user {user} did not replicate to node2",
        )
    try:
        yield
    finally:
        node1.query("DROP USER IF EXISTS u1, u2")


def wait_for(predicate, message):
    for _ in range(POLL_ATTEMPTS):
        if predicate():
            return
        time.sleep(POLL_INTERVAL)
    raise AssertionError(message)


def running(node, query_id):
    return node.query(
        f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'"
    ).strip()


def start_victim(node, user, query_id):
    request = node.get_query_request(VICTIM_QUERY, user=user, query_id=query_id)
    # Observed as the default user, which holds the grants the users under test lack.
    wait_for(
        lambda: running(node, query_id) == "1",
        f"victim {query_id} never appeared in system.processes",
    )
    return request


def drop_victim(node, query_id, request):
    node.query(f"KILL QUERY WHERE query_id = '{query_id}' ASYNC")
    try:
        request.get_answer_and_error()
    except Exception:
        pass


def test_on_cluster_self_kill_default_settings(started_cluster):
    """The victim runs on node2 and u1 kills it from node1 at stock server settings, so each
    worker executes the queued statement with no bound user, that is, with full access."""
    request = start_victim(node2, "u1", "on_cluster_self")
    try:
        node1.query(
            "KILL QUERY ON CLUSTER default WHERE query_id = 'on_cluster_self' SYNC",
            user="u1",
        )
        wait_for(
            lambda: running(node2, "on_cluster_self") == "0",
            "the victim on node2 was not killed",
        )
    finally:
        drop_victim(node2, "on_cluster_self", request)


def test_on_cluster_does_not_kill_other_users_query(started_cluster):
    """The security arm: u1 must not reach u2's query through the relaxed path, even though the
    workers run with full access."""
    request = start_victim(node2, "u2", "on_cluster_other")
    try:
        node1.query(
            "KILL QUERY ON CLUSTER default WHERE query_id = 'on_cluster_other' SYNC",
            user="u1",
        )
        time.sleep(2)
        assert running(node2, "on_cluster_other") == "1"
    finally:
        drop_victim(node2, "on_cluster_other", request)


def test_on_cluster_self_kill_with_initial_user_setting(started_cluster):
    """The same kill with `distributed_ddl_use_initial_user_and_roles` on, which makes the worker
    run as u1 and take the by-id path itself. That configuration needs BOTH knobs: at the default
    `distributed_ddl_entry_format_version` the initiator's identity is not serialized at all, so
    the worker would bind no user and this arm would pass on the ordinary `system.processes` path
    without ever reaching the matcher."""
    set_initial_user_setting("0", "1")
    try:
        request = start_victim(node2, "u1", "on_cluster_initial_user")
        try:
            node1.query(
                "KILL QUERY ON CLUSTER default WHERE query_id = 'on_cluster_initial_user' SYNC",
                user="u1",
                settings={"distributed_ddl_entry_format_version": 8},
            )
            wait_for(
                lambda: running(node2, "on_cluster_initial_user") == "0",
                "the victim on node2 was not killed",
            )
        finally:
            drop_victim(node2, "on_cluster_initial_user", request)
    finally:
        set_initial_user_setting("1", "0")


def set_initial_user_setting(old, new):
    tag = "distributed_ddl_use_initial_user_and_roles"
    for node in all_nodes:
        node.replace_in_config(
            "/etc/clickhouse-server/config.d/config.xml",
            f"<{tag}>{old}</{tag}>",
            f"<{tag}>{new}</{tag}>",
        )
        node.restart_clickhouse()


def test_on_cluster_requires_cluster_grant(started_cluster):
    """The relaxation drops only KILL QUERY; CLUSTER is still required for every ON CLUSTER
    statement."""
    node1.query("REVOKE CLUSTER ON *.* FROM u1")
    error = node1.query_and_get_error(
        "KILL QUERY ON CLUSTER default WHERE query_id = 'no_cluster_grant' SYNC",
        user="u1",
    )
    assert "ACCESS_DENIED" in error
