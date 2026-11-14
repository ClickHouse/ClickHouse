#!/usr/bin/env python3

import time
import uuid

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
# stop_clickhouse silently raises any graceful bound below 180s to exactly 180s on LLVM
# coverage builds, so slow builds must ask for 180: at that value its `stop_wait_sec < 180`
# guard is false, so no override can apply and both legs below enforce the same budget.
SLOW_BUILD_SHUTDOWN_BOUND_SEC = 180


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


def assert_slow_index_build_is_still_in_flight(table):
    # Everything below re-asserts, immediately before the shutdown, that the state this arm
    # needs actually holds. Without it the arm passes for the wrong reason whenever the
    # failpoint is ineffective - e.g. a build without USE_LIBFIU, where FailPoint.h expands
    # fiu_do_on to nothing and the whole injection machinery is compiled out - or whenever
    # the merge happened to finish early: the shutdown is then fast and the assertion holds
    # having never reached the interruption point under test.
    assert (
        node.query(
            "SELECT count() FROM system.fail_points "
            "WHERE name = 'vector_similarity_index_slow_add' AND enabled"
        ).strip()
        == "1"
    ), (
        "failpoint vector_similarity_index_slow_add is not enabled: either the failpoint "
        "was never registered (a build without USE_LIBFIU) or SYSTEM ENABLE FAILPOINT did "
        "not take effect, so this arm cannot exercise the interruption point"
    )
    assert (
        node.query(f"SELECT count() FROM system.merges WHERE table = '{table}'").strip()
        != "0"
    ), "the background merge is no longer running: nothing is left to interrupt"
    assert (
        node.query(
            f"SELECT count() FROM system.parts WHERE table = '{table}' AND active"
        ).strip()
        == "2"
    ), "the merge already completed: the failpoint did not slow the index build"


def test_shutdown_interrupts_vector_index_build(start_cluster):
    # stop_clickhouse returning True only means the process is gone, by any means - not that
    # shutdown ran to completion - so the oracle here is the log marker the server writes
    # after the wait that stalls, and the bound is only a budget. 180 is the slow-build value
    # because stop_clickhouse rewrites any smaller graceful bound to 180 on LLVM coverage
    # builds; at 180 its `stop_wait_sec < 180` guard is false, so both legs share one budget.
    slow_build = node.is_built_with_sanitizer() or node.is_built_with_llvm_coverage()
    bound = SLOW_BUILD_SHUTDOWN_BOUND_SEC if slow_build else SHUTDOWN_BOUND_SEC
    try:
        create_table_and_parts("vec_shutdown")
        node.query("SYSTEM ENABLE FAILPOINT vector_similarity_index_slow_add")
        start_background_merge_and_wait_until_running("vec_shutdown")
        time.sleep(3)
        assert_slow_index_build_is_still_in_flight("vec_shutdown")
        # Start clean so only this shutdown's lines can satisfy the assertions below. After
        # the precondition, so its own queries are not what gets truncated away.
        node.exec_in_container(
            ["bash", "-c", ": > /var/log/clickhouse-server/clickhouse-server.log"]
        )

        started_at = time.time()
        node.stop_clickhouse(stop_wait_sec=bound)
        elapsed = time.time() - started_at
        # Server.cpp logs this after Context::shutdown returns, i.e. strictly after the
        # merges-executor wait that stalls, so a wedged server can never emit it. from_host
        # reads the bind-mounted copy, which survives even if the container dies;
        # only_latest keeps the scan off the rotated files, which rotateOnOpen fills with
        # earlier arms' shutdowns and which would satisfy both assertions on their own.
        assert node.grep_in_log(
            "Background threads finished", from_host=True, only_latest=True
        ), (
            "shutdown did not run to completion while a background merge was building a "
            "vector similarity index"
        )
        assert node.grep_in_log(
            "Cancelled building vector similarity index",
            from_host=True,
            only_latest=True,
        ), "the index build was never interrupted, so this arm did not reach the fix"
        assert elapsed < bound, f"shutdown took {elapsed:.1f}s, bound {bound}s"
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
    # Fresh ids per run: pytest --count re-runs this function against the same server, so a
    # fixed id would read the previous iteration's row out of system.query_log.
    indexed_query_id = f"vec_healthy_indexed_{uuid.uuid4().hex}"
    brute_force_query_id = f"vec_healthy_brute_{uuid.uuid4().hex}"
    with_index = node.query(
        query,
        query_id=indexed_query_id,
        settings={
            "allow_experimental_vector_similarity_index": 1,
            "use_skip_indexes": 1,
        },
    )
    brute_force = node.query(
        query,
        query_id=brute_force_query_id,
        settings={
            "allow_experimental_vector_similarity_index": 1,
            "use_skip_indexes": 0,
        },
    )
    assert with_index == brute_force
    # Equality alone does not show the index was used: a vector index only prunes granules,
    # so a query that never consulted it returns exactly the same rows. USearchSearchCount is
    # incremented once per real index search (MergeTreeIndexVectorSimilarity.cpp), and the
    # PAIR is the discriminator - > 0 alone cannot separate a used index from a counter leaked
    # by another query, and == 0 alone cannot separate a pruned scan from a broken query.
    # Precedent: 02354_vector_search_distributed_index_analysis,
    # 02354_vector_search_concurrent_readers.
    node.query("SYSTEM FLUSH LOGS")
    searches = node.query(
        "SELECT query_id, ProfileEvents['USearchSearchCount'] FROM system.query_log "
        f"WHERE query_id IN ('{indexed_query_id}', '{brute_force_query_id}') "
        "AND type = 'QueryFinish' ORDER BY query_id"
    )
    counts = dict(line.split("\t") for line in searches.strip().split("\n"))
    assert int(counts[brute_force_query_id]) == 0, (
        f"the brute-force control searched the vector index {counts[brute_force_query_id]} "
        "times, so it is not a control"
    )
    assert int(counts[indexed_query_id]) > 0, (
        "the indexed query performed no vector index search, so the equality above says "
        "nothing about the merged index"
    )
    node.query("DROP TABLE vec_healthy SYNC")


def test_shutdown_still_flushes_buffer_table_into_indexed_destination(start_cluster):
    # The interruption point must not fire on the writes that shutdown itself performs.
    # ContextSharedPart::shutdown sets the shutdown flag as its FIRST statement and only
    # afterwards flushes Buffer tables (via DatabaseCatalog::shutdown ->
    # StorageBuffer::flushAndPrepareForShutdown -> optimize -> writeBlockToDestination),
    # which inserts into the destination MergeTree and therefore builds its vector
    # similarity index. That flush is the last chance to persist the buffered rows -
    # flushBuffer returns the block to the in-memory buffer and rethrows on the premise
    # that a later write will retry, flushAndPrepareForShutdown swallows the exception,
    # and StorageBuffer has no shutdown() override - so an abort here loses the rows
    # silently. No failpoint: the point is that an ordinary flush must not be aborted.
    assert (
        node.query(
            "SELECT count() FROM system.fail_points "
            "WHERE name = 'vector_similarity_index_slow_add' AND enabled"
        ).strip()
        == "0"
    ), "the failpoint must be disabled here: this arm measures an ordinary, fast flush"
    node.query("DROP TABLE IF EXISTS vec_buf SYNC")
    node.query("DROP TABLE IF EXISTS vec_buf_dst SYNC")
    node.query(
        f"""
        CREATE TABLE vec_buf_dst (id UInt64, vec Array(Float32),
                                  INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', {DIMENSIONS}))
        ENGINE = MergeTree ORDER BY id
        SETTINGS min_bytes_for_wide_part = 0
        """,
        settings={"allow_experimental_vector_similarity_index": 1},
    )
    # Every threshold is far above what this arm writes, so nothing flushes before the
    # shutdown does. max_rows/max_bytes must also exceed the inserted block, otherwise
    # BufferSink::consume skips the buffer and writes straight through.
    node.query(
        "CREATE TABLE vec_buf AS vec_buf_dst "
        "ENGINE = Buffer(currentDatabase(), vec_buf_dst, 1, 3600, 3600, "
        "1000000, 1000000, 100000000, 1000000000)"
    )
    node.query(
        f"INSERT INTO vec_buf SELECT number AS id, "
        f"arrayMap(x -> toFloat32(id + x), range({DIMENSIONS})) FROM numbers({ROWS})"
    )
    assert node.query("SELECT count() FROM vec_buf_dst").strip() == "0", (
        "the rows reached the destination before the shutdown, so this arm would pass "
        "whether or not the flush is aborted"
    )
    assert node.query("SELECT count() FROM vec_buf").strip() == str(ROWS)

    node.restart_clickhouse()

    # The oracle: with an over-broad interruption point this reads 0.
    assert node.query("SELECT count() FROM vec_buf_dst").strip() == str(ROWS), (
        "the shutdown flush of the Buffer table was aborted, so the buffered rows were "
        "lost instead of being persisted into the indexed destination table"
    )
    node.query("DROP TABLE vec_buf SYNC")
    node.query("DROP TABLE vec_buf_dst SYNC")
