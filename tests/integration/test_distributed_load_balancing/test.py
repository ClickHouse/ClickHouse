# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

import threading
import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

n1 = cluster.add_instance(
    "n1",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/users.xml"],
)
n2 = cluster.add_instance(
    "n2",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/users.xml"],
)
n3 = cluster.add_instance(
    "n3",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/users.xml"],
)

nodes = len(cluster.instances)
queries = nodes * 10


# SYSTEM RELOAD CONFIG will reset some attributes of the nodes in cluster
# - error_count
# - last_used (round_robing)
#
# This is required to avoid interference results of one test to another
@pytest.fixture(scope="function", autouse=True)
def test_setup():
    for n in list(cluster.instances.values()):
        n.query("SYSTEM RELOAD CONFIG")


def bootstrap():
    for n in list(cluster.instances.values()):
        n.query("DROP TABLE IF EXISTS data")
        n.query("DROP TABLE IF EXISTS dist")
        n.query("CREATE TABLE data (key Int) Engine=Memory()")
        n.query(
            """
        CREATE TABLE dist AS data
        Engine=Distributed(
            replicas_cluster,
            currentDatabase(),
            data)
        """
        )
        n.query(
            """
        CREATE TABLE dist_priority AS data
        Engine=Distributed(
            replicas_priority_cluster,
            currentDatabase(),
            data)
        """
        )
        n.query(
            """
        CREATE TABLE dist_priority_negative AS data
        Engine=Distributed(
            replicas_priority_negative_cluster,
            currentDatabase(),
            data)
        """
        )


def make_uuid():
    return uuid.uuid4().hex


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        bootstrap()
        yield cluster
    finally:
        cluster.shutdown()


def get_node(query_node, table="dist", *args, **kwargs):
    query_id = make_uuid()

    settings = {
        "query_id": query_id,
        "log_queries": 1,
        "log_queries_min_type": "QUERY_START",
        "prefer_localhost_replica": 0,
        "max_parallel_replicas": 1,
    }
    if "settings" not in kwargs:
        kwargs["settings"] = settings
    else:
        kwargs["settings"].update(settings)

    query_node.query("SELECT * FROM " + table, *args, **kwargs)

    for n in list(cluster.instances.values()):
        n.query("SYSTEM FLUSH LOGS")

    rows = query_node.query(
        """
    SELECT hostName()
    FROM cluster(shards_cluster, system.query_log)
    WHERE
        initial_query_id = '{query_id}' AND
        is_initial_query = 0 AND
        type = 'QueryFinish'
    ORDER BY event_date DESC, event_time DESC
    LIMIT 1
    """.format(
            query_id=query_id
        )
    )
    return rows.strip()


# TODO: right now random distribution looks bad, but works
def test_load_balancing_default():
    unique_nodes = set()
    for _ in range(0, queries):
        unique_nodes.add(get_node(n1, settings={"load_balancing": "random"}))
    assert len(unique_nodes) == nodes, unique_nodes


def test_load_balancing_nearest_hostname():
    unique_nodes = set()
    for _ in range(0, queries):
        unique_nodes.add(get_node(n1, settings={"load_balancing": "nearest_hostname"}))
    assert len(unique_nodes) == 1, unique_nodes
    assert unique_nodes == set(["n1"])


def test_load_balancing_hostname_levenshtein_distance():
    unique_nodes = set()
    for _ in range(0, queries):
        unique_nodes.add(
            get_node(n1, settings={"load_balancing": "hostname_levenshtein_distance"})
        )
    assert len(unique_nodes) == 1, unique_nodes
    assert unique_nodes == set(["n1"])


def test_load_balancing_hostname_longest_common_prefix():
    unique_nodes = set()
    for _ in range(0, queries):
        unique_nodes.add(
            get_node(n1, settings={"load_balancing": "hostname_longest_common_prefix"})
        )
    assert len(unique_nodes) == 1, unique_nodes
    assert unique_nodes == set(["n1"])


def test_load_balancing_hostname_longest_common_suffix():
    unique_nodes = set()
    for _ in range(0, queries):
        unique_nodes.add(
            get_node(n1, settings={"load_balancing": "hostname_longest_common_suffix"})
        )
    assert len(unique_nodes) == 1, unique_nodes
    assert unique_nodes == set(["n1"])


def test_load_balancing_in_order():
    unique_nodes = set()
    for _ in range(0, queries):
        unique_nodes.add(get_node(n1, settings={"load_balancing": "in_order"}))
    assert len(unique_nodes) == 1, unique_nodes
    assert unique_nodes == set(["n1"])


def test_load_balancing_first_or_random():
    unique_nodes = set()
    for _ in range(0, queries):
        unique_nodes.add(get_node(n1, settings={"load_balancing": "first_or_random"}))
    assert len(unique_nodes) == 1, unique_nodes
    assert unique_nodes == set(["n1"])


def test_load_balancing_round_robin():
    unique_nodes = set()
    for _ in range(0, nodes):
        unique_nodes.add(get_node(n1, settings={"load_balancing": "round_robin"}))
    assert len(unique_nodes) == nodes, unique_nodes
    assert unique_nodes == set(["n1", "n2", "n3"])


# When all replicas are idle, least_request degenerates to random.
def test_load_balancing_least_request_idle():
    unique_nodes = set()
    for _ in range(0, queries):
        unique_nodes.add(get_node(n1, settings={"load_balancing": "least_request"}))
    assert len(unique_nodes) == nodes, unique_nodes


def test_load_balancing_least_request_avoids_busy_replica():
    # The pinning query sleeps 1 second per row on the remote side, so this is
    # the upper bound for the busy window. The test finishes much earlier: the
    # query is killed as soon as the probe queries are done.
    busy_rows = 300
    busy_query_id = make_uuid()
    busy_settings = {
        "query_id": busy_query_id,
        # pin the remote part of the query to n1
        "load_balancing": "in_order",
        "prefer_localhost_replica": 0,
        "max_parallel_replicas": 1,
        # do not let a hedged connection to another replica steal the query from n1
        "use_hedged_requests": 0,
        # sleepEachRow(1) over a whole block exceeds the default 3s per-block
        # sleep cap and would throw TOO_SLOW immediately; 0 disables the cap
        "function_sleep_max_microseconds_per_block": 0,
    }

    def run_busy_query():
        try:
            n1.query("SELECT sleepEachRow(1) FROM dist", settings=busy_settings)
        except Exception:
            pass  # killed below

    n1.query("INSERT INTO data (key) SELECT * FROM numbers({})".format(busy_rows))
    busy_thread = threading.Thread(target=run_busy_query)
    busy_thread.start()

    try:
        # Wait until the remote part of the pinning query is running on n1, i.e.
        # the initiator n1 holds a checked out (in-flight) connection to n1.
        deadline = time.monotonic() + 60
        while (
            int(
                n1.query(
                    "SELECT count() FROM system.processes WHERE query_id != initial_query_id AND initial_query_id = '{}'".format(
                        busy_query_id
                    )
                )
            )
            == 0
        ):
            assert time.monotonic() < deadline, "the pinning query did not start"
            time.sleep(0.1)

        # With choice_count >= number of replicas all replicas are examined
        # (the degenerate full scan case), so every probe query must
        # deterministically avoid n1 while n1 has an in-flight query.
        probe_ids = []
        for _ in range(0, queries):
            query_id = make_uuid()
            n1.query(
                "SELECT * FROM dist",
                settings={
                    "query_id": query_id,
                    "log_queries": 1,
                    "log_queries_min_type": "QUERY_START",
                    "load_balancing": "least_request",
                    "load_balancing_least_request_choice_count": 100,
                    "prefer_localhost_replica": 0,
                    "max_parallel_replicas": 1,
                    "use_hedged_requests": 0,
                },
            )
            probe_ids.append(query_id)
    finally:
        n1.query("KILL QUERY WHERE query_id = '{}' SYNC".format(busy_query_id))
        busy_thread.join()
        n1.query("TRUNCATE TABLE data")

    for n in list(cluster.instances.values()):
        n.query("SYSTEM FLUSH LOGS")

    rows = n1.query(
        """
    SELECT hostName(), count()
    FROM cluster(shards_cluster, system.query_log)
    WHERE
        initial_query_id IN ({query_ids}) AND
        is_initial_query = 0 AND
        type = 'QueryFinish'
    GROUP BY 1
    ORDER BY 1
    """.format(
            query_ids=",".join("'{}'".format(query_id) for query_id in probe_ids)
        )
    )
    queries_per_node = dict(
        (line.split("\t")[0], int(line.split("\t")[1]))
        for line in rows.strip().split("\n")
    )
    assert "n1" not in queries_per_node, queries_per_node
    assert sum(queries_per_node.values()) == len(probe_ids), queries_per_node


@pytest.mark.parametrize(
    "dist_table",
    [
        ("dist_priority"),
        ("dist_priority_negative"),
    ],
)
def test_load_balancing_priority_round_robin(dist_table):
    unique_nodes = set()
    for _ in range(0, nodes):
        unique_nodes.add(
            get_node(n1, dist_table, settings={"load_balancing": "round_robin"})
        )
    assert len(unique_nodes) == 2, unique_nodes
    # n2 has bigger priority in config
    assert unique_nodes == set(["n1", "n3"])


def test_distributed_replica_max_ignored_errors():
    settings = {
        "use_hedged_requests": 0,
        "load_balancing": "in_order",
        "prefer_localhost_replica": 0,
        "connect_timeout": 2,
        "receive_timeout": 2,
        "send_timeout": 2,
        "tcp_keep_alive_timeout": 2,
        "distributed_replica_max_ignored_errors": 0,
        "distributed_replica_error_half_life": 60,
        "max_parallel_replicas": 1,
    }

    # initiate connection (if started only this test)
    n2.query("SELECT * FROM dist", settings=settings)

    with cluster.pause_container("n1"):
        # n1 paused -- skipping, and increment error_count for n1
        # but the query succeeds, no need in query_and_get_error()
        n2.query("SELECT * FROM dist", settings=settings)
        # XXX: due to config reloading we need second time (sigh)
        n2.query("SELECT * FROM dist", settings=settings)
        # check error_count for n1
        assert (
            int(
                n2.query(
                    """
        SELECT errors_count FROM system.clusters
        WHERE cluster = 'replicas_cluster' AND host_name = 'n1'
        """,
                    settings=settings,
                )
            )
            == 1
        )

    # still n2
    assert get_node(n2, settings=settings) == "n2"
    # now n1
    settings["distributed_replica_max_ignored_errors"] = 1
    assert get_node(n2, settings=settings) == "n1"
