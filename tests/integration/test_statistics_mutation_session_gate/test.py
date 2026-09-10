# https://github.com/ClickHouse/ClickHouse/issues/115448
#
# `allow_statistics` gates the statistics DDL. It used to be checked twice: once when the statement
# was validated, with the submitting session's value, and once more while the mutation ran - where
# the background context reads the server-default profile rather than that session. With
# `allow_statistics = 0` in the profile and `allow_statistics = 1` in the statement, the mutation was
# therefore accepted and then failed on every retry, sitting in `system.mutations` with
# `is_done = 0` forever and blocking every later mutation on the table.
#
# The server profile is what a stateless test cannot set, which is why this half of the reproducer
# lives here; `05175_materialize_statistics_session_gate` pins the submission-time rejection.

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    user_configs=["config/disable_statistics.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def unfinished_mutations(table):
    return node.query(
        f"SELECT countIf(is_done = 0) FROM system.mutations WHERE database = 'default' AND table = '{table}'"
    ).strip()


def test_statistics_mutations_with_session_opt_in(started_cluster):
    node.query("DROP TABLE IF EXISTS t_session_gate SYNC")
    node.query(
        """
        CREATE TABLE t_session_gate (k UInt64, a Int64) ENGINE = MergeTree ORDER BY k
        """
    )
    node.query("INSERT INTO t_session_gate SELECT number, number FROM numbers(100)")

    # `ADD STATISTICS` has always honoured the session opt-in.
    node.query(
        "ALTER TABLE t_session_gate ADD STATISTICS a TYPE tdigest",
        settings={"allow_statistics": 1, "mutations_sync": 2},
    )

    # `MATERIALIZE STATISTICS` used to be accepted here and then fail forever in the background.
    node.query(
        "ALTER TABLE t_session_gate MATERIALIZE STATISTICS a",
        settings={"allow_statistics": 1, "mutations_sync": 2},
    )
    assert unfinished_mutations("t_session_gate") == "0"

    node.query(
        "ALTER TABLE t_session_gate DROP STATISTICS a",
        settings={"allow_statistics": 1, "mutations_sync": 2},
    )
    assert unfinished_mutations("t_session_gate") == "0"

    # The table is not wedged: a later, unrelated mutation still completes.
    node.query(
        "ALTER TABLE t_session_gate UPDATE a = a + 1 WHERE k < 10",
        settings={"mutations_sync": 2},
    )
    assert unfinished_mutations("t_session_gate") == "0"
    assert node.query("SELECT count() FROM t_session_gate").strip() == "100"

    node.query("DROP TABLE t_session_gate SYNC")


def test_statistics_mutations_without_session_opt_in(started_cluster):
    node.query("DROP TABLE IF EXISTS t_session_gate_off SYNC")
    node.query(
        """
        CREATE TABLE t_session_gate_off (k UInt64, a Int64) ENGINE = MergeTree ORDER BY k
        """
    )

    # Without the opt-in the profile's value still refuses the statements up front, and nothing
    # reaches the mutation queue - including when the query-shape validation is turned off.
    for settings in ({}, {"validate_mutation_query": 0}):
        for statement in (
            "ALTER TABLE t_session_gate_off ADD STATISTICS a TYPE tdigest",
            "ALTER TABLE t_session_gate_off MATERIALIZE STATISTICS a",
            "ALTER TABLE t_session_gate_off DROP STATISTICS a",
        ):
            assert "Alter table with statistics is disabled" in node.query_and_get_error(
                statement, settings=settings
            )

    assert unfinished_mutations("t_session_gate_off") == "0"
    node.query("DROP TABLE t_session_gate_off SYNC")
