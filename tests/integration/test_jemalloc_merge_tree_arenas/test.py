#!/usr/bin/env python3

# Exercises the `jemalloc_merge_tree_arenas` server setting, which controls the dedicated
# jemalloc arena pool for long-lived MergeTree metadata. It is a startup-only server setting, so
# each value needs its own server instance. We assert:
#   - the setting is read (system.server_settings),
#   - the resulting arena count is exposed via jemalloc.mergetree_arena.count and matches the value
#     (0 disabled, 1 single, N sharded, capped at the CPU core count),
#   - routing follows the count (disabled -> no active_bytes metric; a pool -> arena fills up).

import re

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node_disabled = cluster.add_instance("node_disabled", main_configs=["configs/disabled.xml"])
node_single = cluster.add_instance("node_single", main_configs=["configs/single.xml"])
node_pool = cluster.add_instance("node_pool", main_configs=["configs/pool.xml"])
node_capped = cluster.add_instance("node_capped", main_configs=["configs/capped.xml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def jemalloc_built_in(node):
    return (
        node.query(
            "SELECT value IN ('ON', '1') FROM system.build_options WHERE name = 'USE_JEMALLOC'"
        ).strip()
        == "1"
    )


def configured_value(node):
    return int(
        node.query(
            "SELECT value FROM system.server_settings WHERE name = 'jemalloc_merge_tree_arenas'"
        ).strip()
    )


def arena_count(node):
    node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS")
    return int(
        node.query(
            "SELECT value FROM system.asynchronous_metrics WHERE metric = 'jemalloc.mergetree_arena.count'"
        ).strip()
    )


def num_cpus(node):
    return int(node.exec_in_container(["nproc"]).strip())


def test_setting_is_read(started_cluster):
    assert configured_value(node_disabled) == 0
    assert configured_value(node_single) == 1
    assert configured_value(node_pool) == 4
    assert configured_value(node_capped) == 1000000


def test_arena_count_matches_setting(started_cluster):
    if not jemalloc_built_in(node_single):
        pytest.skip("built without jemalloc")

    assert arena_count(node_disabled) == 0
    assert arena_count(node_single) == 1
    # Capped at the number of CPU cores the container sees.
    assert arena_count(node_pool) == min(4, num_cpus(node_pool))
    # A value far above the core count collapses to the number of CPUs the container may run on,
    # proving the cap.
    capped = arena_count(node_capped)
    assert capped == min(1000000, num_cpus(node_capped))


def test_disabled_does_not_route_to_a_dedicated_arena(started_cluster):
    if not jemalloc_built_in(node_disabled):
        pytest.skip("built without jemalloc")

    node_disabled.query("DROP TABLE IF EXISTS t SYNC")
    node_disabled.query("CREATE TABLE t (a UInt64, b String) ENGINE = MergeTree ORDER BY a")
    node_disabled.query("INSERT INTO t SELECT number, toString(number) FROM numbers(100000)")
    node_disabled.query("SYSTEM RELOAD ASYNCHRONOUS METRICS")

    # With the pool disabled, the per-arena byte metrics are not emitted at all.
    assert (
        node_disabled.query(
            "SELECT count() FROM system.asynchronous_metrics WHERE metric = 'jemalloc.mergetree_arena.active_bytes'"
        ).strip()
        == "0"
    )


def test_pool_arena_accumulates(started_cluster):
    if not jemalloc_built_in(node_pool):
        pytest.skip("built without jemalloc")

    node_pool.query("DROP TABLE IF EXISTS t SYNC")
    node_pool.query("CREATE TABLE t (a UInt64, b String) ENGINE = MergeTree ORDER BY a")
    node_pool.query("INSERT INTO t SELECT number, toString(number) FROM numbers(100000)")
    node_pool.query("SYSTEM RELOAD ASYNCHRONOUS METRICS")

    active_bytes = int(
        node_pool.query(
            "SELECT value FROM system.asynchronous_metrics WHERE metric = 'jemalloc.mergetree_arena.active_bytes'"
        ).strip()
    )
    assert active_bytes > 0


def manual_arena_nmalloc(node):
    # Cumulative allocation count ("nmalloc") per manually-created arena from malloc_stats_print.
    # On node_pool those are exactly the MergeTree pool arenas: the cache arena is disabled in
    # pool.xml and the queries disable expression compilation so no JIT arena appears. The auto
    # (per-CPU) arenas used for transient allocations are excluded. nmalloc only ever grows, so a
    # per-arena delta reliably shows which pool arenas received allocations, independent of how much
    # is retained or freed by background purge.
    text = node.query("SELECT stats FROM system.jemalloc_stats FORMAT TSVRaw")
    result = {}
    for block in re.split(r"\narenas\[", text)[1:]:
        index_match = re.match(r"(\d+)\]", block)
        if not index_match or not re.search(r'name:\s*"manual', block):
            continue
        # total row columns: allocated  nmalloc  (#/sec)  ndalloc  ...
        total_match = re.search(r"\ntotal:\s+\d+\s+(\d+)", block)
        if total_match:
            result[int(index_match.group(1))] = int(total_match.group(1))
    return result


def test_pool_shards_across_arenas(started_cluster):
    if not jemalloc_built_in(node_pool):
        pytest.skip("built without jemalloc")
    if arena_count(node_pool) < 2:
        pytest.skip("pool collapsed to a single arena (fewer than 2 routable CPUs)")

    node_pool.query("DROP TABLE IF EXISTS t_shard SYNC")
    node_pool.query(
        "CREATE TABLE t_shard (id UInt64, "
        + ", ".join(f"c{i} String" for i in range(40))
        + ") ENGINE = MergeTree ORDER BY id "
        "SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0"
    )

    before = manual_arena_nmalloc(node_pool)

    # Many small wide parts produce many concurrent background merges, which run on the merge thread
    # pool spread across CPUs, so per-part metadata is allocated from several pool arenas rather than
    # a single one.
    cols = ", ".join(f"toString(number + {i})" for i in range(40))
    for batch in range(20):
        node_pool.query(
            f"INSERT INTO t_shard SELECT number + {batch} * 100000, {cols} FROM numbers(1500)",
            settings={
                "max_insert_block_size": 150,
                "min_insert_block_size_rows": 150,
                # No JIT: expression compilation would lazily create the JIT arena, another
                # "manual" arena that could satisfy the sharding assertion below.
                "compile_expressions": 0,
            },
        )
    for _ in range(5):
        node_pool.query("OPTIMIZE TABLE t_shard FINAL")

    after = manual_arena_nmalloc(node_pool)
    node_pool.query("DROP TABLE t_shard SYNC")

    # Pool arenas that received a non-trivial number of metadata allocations. If routing were
    # broken (every allocation to arena 0) at most one would grow; a working per-CPU pool spreads the
    # per-part metadata across several.
    grew = sorted(idx for idx, n in after.items() if n > before.get(idx, 0) + 500)
    assert len(grew) >= 2, f"metadata landed in only {len(grew)} pool arena(s): {grew}"
