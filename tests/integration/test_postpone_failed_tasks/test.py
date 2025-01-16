import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node_with_backoff = cluster.add_instance(
    "node_with_backoff",
    macros={"cluster": "test_cluster"},
    main_configs=["configs/config.d/backoff_policy.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

node_no_backoff = cluster.add_instance(
    "node_no_backoff",
    macros={"cluster": "test_cluster"},
    main_configs=["configs/config.d/no_backoff_policy.xml"],
    with_zookeeper=True,
)

REPLICATED_POSTPONE_LOG = (
    "According to exponential backoff policy, put aside this log entry"
)
NON_REPLICATED_POSTPONE_MUTATION_LOG = (
    "According to exponential backoff policy, do not perform mutations for the part"
)
FAILING_MUTATION_QUERY = "ALTER TABLE test_table DELETE WHERE x IN (SELECT throwIf(1)) SETTINGS allow_nondeterministic_mutations = 1"

all_nodes = [node_with_backoff, node_no_backoff]


def prepare_cluster(use_replicated_table):
    for node in all_nodes:
        node.query("DROP TABLE IF EXISTS test_table SYNC")

    engine = (
        "ReplicatedMergeTree('/clickhouse/{cluster}/tables/test/test_table', '{instance}')"
        if use_replicated_table
        else "MergeTree()"
    )

    for node in all_nodes:
        node.rotate_logs()
        node.query(f"CREATE TABLE test_table(x UInt32) ENGINE {engine} ORDER BY x")
        node.query("INSERT INTO test_table SELECT * FROM system.numbers LIMIT 10")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


@pytest.mark.parametrize(
    ("node, found_in_log"),
    [
        (
            node_with_backoff,
            True,
        ),
        (
            node_no_backoff,
            False,
        ),
    ],
)
def test_mutation_exponential_backoff_with_merge_tree(
    started_cluster, node, found_in_log
):
    prepare_cluster(False)

    def check_logs():
        if found_in_log:
            assert node.wait_for_log_line(NON_REPLICATED_POSTPONE_MUTATION_LOG)
            # Do not rotate the logs when we are checking the absence of a log message
            node.rotate_logs()
        else:
            # Best effort, but when it fails, then the logs for sure contain the problematic message
            assert not node.contains_in_log(NON_REPLICATED_POSTPONE_MUTATION_LOG)

    # Executing incorrect mutation.
    node.query(FAILING_MUTATION_QUERY)

    check_logs()

    node.query("KILL MUTATION WHERE table='test_table'")
    # Check that after kill new parts mutations are postponing.
    node.query(FAILING_MUTATION_QUERY)

    check_logs()


def count_postponed_tasks_in_replicated_queue(node):
    return int(
        node.query(
            f"SELECT count() FROM system.replication_queue WHERE table='test_table' and postpone_reason LIKE '%{REPLICATED_POSTPONE_LOG}%'"
        ).split()[0]
    )


@pytest.mark.parametrize(
    ("src_node, dst_node, backoff_expected"),
    [
        pytest.param(node_no_backoff, node_with_backoff, True, id="with_backoff"),
        pytest.param(node_with_backoff, node_no_backoff, False, id="without_backoff"),
    ],
)
def test_fetch_exponential_backoff_with_replicated_tree(
    started_cluster, src_node, dst_node, backoff_expected
):

    prepare_cluster(True)
    src_node.query("SYSTEM STOP MERGES test_table")
    dst_node.query("SYSTEM STOP MERGES test_table")
    dst_node.query("SYSTEM STOP FETCHES test_table")

    src_node.query("INSERT INTO test_table SELECT rand()%20 FROM numbers(10)")
    src_node.query("DETACH TABLE test_table")
    dst_node.query("SYSTEM START FETCHES test_table")

    ## The fetch from the src replica will be impossible, until table is detached.
    ## Actually this is an imitation of scenario when one replica inserted the data and immediately becomes unavaliable,
    ## so fethes are impossible.
    retry_count = 200
    task_posponed = False
    for _ in range(0, retry_count):
        if count_postponed_tasks_in_replicated_queue(dst_node):
            task_posponed = True
            break

    assert task_posponed == backoff_expected
    src_node.query("ATTACH TABLE test_table")


def test_mutation_exponential_backoff_with_replicated_tree(started_cluster):
    prepare_cluster(True)

    node_with_backoff.query(FAILING_MUTATION_QUERY)

    assert node_with_backoff.wait_for_log_line(REPLICATED_POSTPONE_LOG)
    assert count_postponed_tasks_in_replicated_queue(node_with_backoff) == 1
    assert not node_no_backoff.contains_in_log(REPLICATED_POSTPONE_LOG)


def test_mutatuion_exponential_backoff_create_dependent_table(started_cluster):
    prepare_cluster(False)

    # Executing incorrect mutation.
    node_with_backoff.query(
        "ALTER TABLE test_table DELETE WHERE x IN (SELECT x FROM dep_table) SETTINGS allow_nondeterministic_mutations = 1, validate_mutation_query = 0"
    )

    # Creating dependent table for mutation.
    node_with_backoff.query(
        "CREATE TABLE dep_table(x UInt32) ENGINE MergeTree() ORDER BY x"
    )

    retry_count = 100
    no_unfinished_mutation = False
    for _ in range(0, retry_count):
        if (
            node_with_backoff.query(
                "SELECT count() FROM system.mutations WHERE is_done=0"
            )
            == "0\n"
        ):
            no_unfinished_mutation = True
            break

    assert no_unfinished_mutation
    node_with_backoff.query("DROP TABLE IF EXISTS dep_table SYNC")


def test_exponential_backoff_setting_override(started_cluster):
    node = node_with_backoff
    node.rotate_logs()
    node.query("DROP TABLE IF EXISTS test_table SYNC")
    node.query(
        "CREATE TABLE test_table(x UInt32) ENGINE=MergeTree() ORDER BY x SETTINGS max_postpone_time_for_failed_mutations_ms=0"
    )
    node.query("INSERT INTO test_table SELECT * FROM system.numbers LIMIT 10")

    # Executing incorrect mutation.
    node.query(FAILING_MUTATION_QUERY)
    assert not node.contains_in_log(NON_REPLICATED_POSTPONE_MUTATION_LOG)


@pytest.mark.parametrize(
    ("replicated_table"),
    [
        (False),
        (True),
    ],
)
def test_backoff_clickhouse_restart(started_cluster, replicated_table):
    prepare_cluster(replicated_table)
    node = node_with_backoff

    # Executing incorrect mutation.
    node.query(FAILING_MUTATION_QUERY)
    assert node.wait_for_log_line(
        REPLICATED_POSTPONE_LOG
        if replicated_table
        else NON_REPLICATED_POSTPONE_MUTATION_LOG
    )

    node.restart_clickhouse()
    node.rotate_logs()

    assert node.wait_for_log_line(
        REPLICATED_POSTPONE_LOG
        if replicated_table
        else NON_REPLICATED_POSTPONE_MUTATION_LOG
    )


@pytest.mark.parametrize(
    ("replicated_table"),
    [
        (False),
        (True),
    ],
)
def test_no_backoff_after_killing_mutation(started_cluster, replicated_table):
    prepare_cluster(replicated_table)
    node = node_with_backoff

    # Executing incorrect mutation.
    node.query(FAILING_MUTATION_QUERY)

    # Executing correct mutation.
    node.query("ALTER TABLE test_table DELETE WHERE x=1")
    assert node.wait_for_log_line(
        REPLICATED_POSTPONE_LOG
        if replicated_table
        else NON_REPLICATED_POSTPONE_MUTATION_LOG
    )
    mutation_ids = node.query("select mutation_id from system.mutations").split()

    node.query(
        f"KILL MUTATION WHERE table = 'test_table' AND mutation_id = '{mutation_ids[0]}'"
    )
    node.rotate_logs()
    assert not node.contains_in_log(
        REPLICATED_POSTPONE_LOG
        if replicated_table
        else NON_REPLICATED_POSTPONE_MUTATION_LOG
    )
