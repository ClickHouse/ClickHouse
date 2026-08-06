#!/usr/bin/env python3

import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node", main_configs=["configs/vector_index_pool.xml"], stay_alive=True
)

# Each indexed row sleeps 50ms under the failpoint. One merge block covers both parts, so
# the merge needs 2 * ROWS * 50ms = 100s - far longer than SHUTDOWN_BOUND_SEC, and it
# cannot finish on its own. The failpoint is enabled only after the INSERTs so building
# the two source parts stays fast.
ROWS = 1000
DIMENSIONS = 4
SHUTDOWN_BOUND_SEC = 30


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_table_and_parts(table):
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"""
        CREATE TABLE {table} (id UInt64, vec Array(Float32),
                              INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', {DIMENSIONS}))
        ENGINE = MergeTree ORDER BY id
        SETTINGS index_granularity = {ROWS}, min_bytes_for_wide_part = 0,
                 merge_max_block_size = {2 * ROWS}, merge_max_block_size_bytes = '100G',
                 min_age_to_force_merge_seconds = 1, min_age_to_force_merge_on_partition_only = 0,
                 enable_vertical_merge_algorithm = 0
        """,
        settings={"allow_experimental_vector_similarity_index": 1},
    )
    # A vertical merge builds the skip index in a separate stage that this fixture does
    # not gate, so the merge finishes in well under a second and the arm would be
    # vacuous. Pin the horizontal algorithm, which is where the index build sits.
    node.query(f"SYSTEM STOP MERGES {table}")
    for offset in (0, 10_000_000):
        # The vector is a function of the *id*, not of `number`: otherwise both parts get
        # byte-identical vectors and every nearest-neighbour query is a tie, which makes
        # the ORDER BY ... LIMIT oracle below return an arbitrary subset.
        node.query(
            f"INSERT INTO {table} SELECT number + {offset} AS id, "
            f"arrayMap(x -> toFloat32(id + x), range({DIMENSIONS})) FROM numbers({ROWS})",
            settings={
                "max_insert_block_size": ROWS,
                "min_insert_block_size_rows": ROWS,
            },
        )
    assert (
        node.query(
            f"SELECT count() FROM system.parts WHERE table = '{table}' AND active"
        ).strip()
        == "2"
    )


def start_background_merge_and_wait_until_running(table):
    # A background merge, not OPTIMIZE: OPTIMIZE executes the merge in the query thread,
    # which has a process list element and is therefore killed by killAllQueries() early
    # in shutdown - i.e. the channel that already worked before this fix.
    node.query(f"SYSTEM START MERGES {table}")
    for _ in range(100):
        if (
            node.query(
                f"SELECT count() FROM system.merges WHERE table = '{table}'"
            ).strip()
            != "0"
        ):
            break
        time.sleep(0.5)
    else:
        raise AssertionError("background merge did not start")
    # The merge must be the slow, failpoint-gated one; if it already finished then the
    # index build was never reached and the arm below would pass for the wrong reason.
    assert (
        node.query(
            f"SELECT count() FROM system.parts WHERE table = '{table}' AND active"
        ).strip()
        == "2"
    ), "merge completed before shutdown: the failpoint did not slow the index build"
    # No foreground query may be driving the merge.
    assert (
        node.query(
            "SELECT count() FROM system.processes WHERE query NOT LIKE '%system.processes%'"
        ).strip()
        == "0"
    )


def test_shutdown_interrupts_vector_index_build(start_cluster):
    try:
        create_table_and_parts("vec_shutdown")
        node.query("SYSTEM ENABLE FAILPOINT vector_similarity_index_slow_add")
        start_background_merge_and_wait_until_running("vec_shutdown")
        time.sleep(3)

        started_at = time.time()
        assert node.stop_clickhouse(stop_wait_sec=SHUTDOWN_BOUND_SEC), (
            f"server did not shut down within {SHUTDOWN_BOUND_SEC}s while a background "
            "merge was building a vector similarity index"
        )
        elapsed = time.time() - started_at
        assert elapsed < SHUTDOWN_BOUND_SEC, f"shutdown took {elapsed:.1f}s"
    finally:
        # The server is stopped on the success path and may be stopped on the failure
        # path too, so restart before issuing any cleanup query.
        node.start_clickhouse()
        node.query("SYSTEM DISABLE FAILPOINT vector_similarity_index_slow_add")
        node.query("DROP TABLE IF EXISTS vec_shutdown SYNC")


def test_uncancelled_merge_still_builds_a_usable_index(start_cluster):
    # Positive control: without shutdown the merge completes and the merged index is
    # still correct, so the interruption point does not abort healthy merges.
    # Disable the failpoint unconditionally: if the test above failed mid-way its server
    # was down when its own cleanup ran, and a still-enabled failpoint would make this
    # merge take ~100s and fail here for an unrelated reason.
    node.query("SYSTEM DISABLE FAILPOINT vector_similarity_index_slow_add")
    create_table_and_parts("vec_healthy")
    node.query("SYSTEM START MERGES vec_healthy")
    for _ in range(120):
        if (
            node.query(
                "SELECT count() FROM system.parts WHERE table = 'vec_healthy' AND active"
            ).strip()
            == "1"
        ):
            break
        time.sleep(0.5)
    else:
        raise AssertionError("merge did not complete")

    assert node.query("SELECT count() FROM vec_healthy").strip() == str(2 * ROWS)
    # Vectors are strictly monotonic in id, and the probe is deliberately off-centre
    # (500.3, not 500.0) so no two rows are equidistant from it. The nearest neighbours
    # are therefore unambiguous and the index result must equal brute force exactly.
    # arraySort, not groupArray alone: groupArray does not preserve the subquery's order,
    # so comparing raw group arrays compares an arbitrary permutation of the same set.
    query = (
        "SELECT arraySort(groupArray(id)) FROM (SELECT id FROM vec_healthy "
        "ORDER BY L2Distance(vec, [500.3, 501.3, 502.3, 503.3]) LIMIT 3)"
    )
    with_index = node.query(
        query,
        settings={
            "allow_experimental_vector_similarity_index": 1,
            "use_skip_indexes": 1,
        },
    )
    brute_force = node.query(
        query,
        settings={
            "allow_experimental_vector_similarity_index": 1,
            "use_skip_indexes": 0,
        },
    )
    assert with_index == brute_force
    node.query("DROP TABLE vec_healthy SYNC")
