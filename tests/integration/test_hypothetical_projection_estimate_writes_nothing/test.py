import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test_estimate_does_not_touch_the_part_directory(started_cluster):
    # An estimate reads the part and must not write to it. It takes its synthetic projection part
    # from the same builder a materialization uses, and that builder reclaims a `<name>.tmp_proj`
    # directory left behind by an interrupted attempt, which a read may not do.
    node.query("DROP TABLE IF EXISTS t_writes SYNC")
    node.query(
        "CREATE TABLE t_writes (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a "
        "SETTINGS index_granularity = 100, min_bytes_for_wide_part = 0"
    )
    node.query("INSERT INTO t_writes SELECT number, number % 100 FROM numbers(1000)")

    part = node.query(
        "SELECT path FROM system.parts WHERE table = 't_writes' AND active"
    ).strip()
    leftover = part + "p_candidate.tmp_proj/leftover"
    node.exec_in_container(
        ["bash", "-c", f"mkdir {part}p_candidate.tmp_proj && echo kept > {leftover}"]
    )

    # the projection has to be defined in the same session as the estimate, it is session-scoped
    estimate = node.query(
        "CREATE HYPOTHETICAL PROJECTION p_candidate ON t_writes (SELECT a, b ORDER BY b);"
        "EXPLAIN WHATIF SELECT count() FROM t_writes WHERE b = 42 SETTINGS "
        "optimize_trivial_count_query = 0, optimize_use_projections = 1, optimize_use_implicit_projections = 0"
    )
    # the scan really ran, so the builder was reached
    assert "empirical" in estimate, estimate

    assert node.exec_in_container(["bash", "-c", f"cat {leftover}"]) == "kept\n"
