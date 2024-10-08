import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node_with_backoff = cluster.add_instance(
    "node_with_backoff",
    macros={"cluster": "test_cluster"},
    main_configs=["configs/config.d/backoff_mutation_policy.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

node_no_backoff = cluster.add_instance(
    "node_no_backoff",
    macros={"cluster": "test_cluster"},
    main_configs=["configs/config.d/no_backoff_mutation_policy.xml"],
    with_zookeeper=True,
)

REPLICATED_POSTPONE_MUTATION_LOG = (
    "According to exponential backoff policy, put aside this log entry"
)
POSTPONE_MUTATION_LOG = (
    "According to exponential backoff policy, do not perform mutations for the part"
)

all_nodes = [node_with_backoff, node_no_backoff]


def prepare_cluster(use_replicated_table):
    for node in all_nodes:
        node.query("DROP TABLE IF EXISTS test_mutations SYNC")

    engine = (
        "ReplicatedMergeTree('/clickhouse/{cluster}/tables/test/test_mutations', '{instance}')"
        if use_replicated_table
        else "MergeTree()"
    )

    for node in all_nodes:
        node.rotate_logs()
        node.query(f"CREATE TABLE test_mutations(x UInt32) ENGINE {engine} ORDER BY x")
        node.query("INSERT INTO test_mutations SELECT * FROM system.numbers LIMIT 10")


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
def test_exponential_backoff_with_merge_tree(started_cluster, node, found_in_log):
    prepare_cluster(False)

    def check_logs():
        if found_in_log:
            assert node.wait_for_log_line(POSTPONE_MUTATION_LOG)
            # Do not rotate the logs when we are checking the absence of a log message
            node.rotate_logs()
        else:
            # Best effort, but when it fails, then the logs for sure contain the problematic message
            assert not node.contains_in_log(POSTPONE_MUTATION_LOG)

    # Executing incorrect mutation.
    node.query(
        "ALTER TABLE test_mutations DELETE WHERE x IN (SELECT x FROM notexist_table) SETTINGS allow_nondeterministic_mutations=1"
    )

    check_logs()

    node.query("KILL MUTATION WHERE table='test_mutations'")
    # Check that after kill new parts mutations are postponing.
    node.query(
        "ALTER TABLE test_mutations DELETE WHERE x IN (SELECT x FROM notexist_table) SETTINGS allow_nondeterministic_mutations=1"
    )

    check_logs()


def test_exponential_backoff_with_replicated_tree(started_cluster):
    prepare_cluster(True)

    node_with_backoff.query(
        "ALTER TABLE test_mutations DELETE WHERE x IN (SELECT x FROM notexist_table) SETTINGS allow_nondeterministic_mutations=1"
    )

    assert node_with_backoff.wait_for_log_line(REPLICATED_POSTPONE_MUTATION_LOG)
    assert not node_no_backoff.contains_in_log(REPLICATED_POSTPONE_MUTATION_LOG)


def test_exponential_backoff_create_dependent_table(started_cluster):
    prepare_cluster(False)

    # Executing incorrect mutation.
    node_with_backoff.query(
        "ALTER TABLE test_mutations DELETE WHERE x IN (SELECT x  FROM dep_table) SETTINGS allow_nondeterministic_mutations=1"
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
    node.query("DROP TABLE IF EXISTS test_mutations SYNC")
    node.query(
        "CREATE TABLE test_mutations(x UInt32) ENGINE=MergeTree() ORDER BY x SETTINGS max_postpone_time_for_failed_mutations_ms=0"
    )
    node.query("INSERT INTO test_mutations SELECT * FROM system.numbers LIMIT 10")

    # Executing incorrect mutation.
    node.query(
        "ALTER TABLE test_mutations DELETE WHERE x IN (SELECT x  FROM dep_table) SETTINGS allow_nondeterministic_mutations=1"
    )
    assert not node.contains_in_log(POSTPONE_MUTATION_LOG)


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
    node.query(
        "ALTER TABLE test_mutations DELETE WHERE x IN (SELECT x  FROM dep_table) SETTINGS allow_nondeterministic_mutations=1"
    )
    assert node.wait_for_log_line(
        REPLICATED_POSTPONE_MUTATION_LOG if replicated_table else POSTPONE_MUTATION_LOG
    )

    node.restart_clickhouse()
    node.rotate_logs()

    assert node.wait_for_log_line(
        REPLICATED_POSTPONE_MUTATION_LOG if replicated_table else POSTPONE_MUTATION_LOG
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
    node.query(
        "ALTER TABLE test_mutations DELETE WHERE x IN (SELECT x  FROM dep_table) SETTINGS allow_nondeterministic_mutations=1"
    )
    # Executing correct mutation.
    node.query("ALTER TABLE test_mutations DELETE  WHERE x=1")
    assert node.wait_for_log_line(
        REPLICATED_POSTPONE_MUTATION_LOG if replicated_table else POSTPONE_MUTATION_LOG
    )
    mutation_ids = node.query("select mutation_id from system.mutations").split()

    node.query(
        f"KILL MUTATION WHERE table = 'test_mutations' AND mutation_id = '{mutation_ids[0]}'"
    )
    node.rotate_logs()
    assert not node.contains_in_log(
        REPLICATED_POSTPONE_MUTATION_LOG if replicated_table else POSTPONE_MUTATION_LOG
    )
