"""
Regression test for `AggregateFunction(topK(N), String)` state compatibility
across versions.

Background. Up to and including 25.10, `SpaceSaving::readAlphaMap` populated
the alpha map via `push_back` while `AggregateFunctionTopKGeneric::deserialize`
had already pre-allocated it through `set.resize(min(size + 1, reserved))`.
Every read+rewrite cycle therefore wrote
    nextAlphaSize(min(size + 1, reserved)) + alpha_size_from_file
back to disk, so any `AggregateFunction(topK(N), String)` state that survived
even one merge or mutation under those versions ended up with an `alpha_size`
that is no longer a power of two.

#90091 (shipped in 25.12) overwrites the alpha map in place and adds a strict
equality check between the on-disk `alpha_size` and `alpha_map.size()`. That
check rejects every part written by a pre-fix version, even though the
counter list itself is intact.

Reproduction. The corruption only persists on disk when a state is
deserialized and re-serialized without `AggregateFunctionTopKGeneric::merge`
running in between (because that path calls `set.resize(reserved)`, which
truncates the corrupted alpha map back to the right size before write).
A regular merge of distinct primary keys hits `ColumnAggregateFunction::
ensureOwnership` which calls `func->merge` for every row and washes the
corruption out before the data ever lands on disk.

The pattern that does *not* go through `merge` is a mutation on a **compact**
part touching a *different* column: the layout forces a full read+write of
the aggregate state column, but no row is combined with another, so `merge`
is never called. We use that here:

    * write data on 25.10 (buggy `readAlphaMap`, no strict check)
    * `ALTER TABLE ... UPDATE other_col = ...` on a compact part — this
      rewrites every column including the topK state, propagating the
      grown `alpha_size` onto disk
    * upgrade to the latest binary

Without the read-side compatibility fix the SELECT throws
`SIZES_OF_ARRAYS_DONT_MATCH`; with the fix it reads the 20 distinct keys
that were inserted into the state.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    image="clickhouse/clickhouse-server",
    tag="25.10",
    with_installed_binary=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _create_and_corrupt(node):
    node.query("DROP TABLE IF EXISTS t_topk SYNC;")
    # Force the part into compact format so a mutation on `meta` rewrites the
    # whole `data.bin`, including the `s` column. Wide parts would hardlink
    # `s.bin` and never trigger the buggy deserialize+serialize cycle.
    node.query(
        """
        CREATE TABLE t_topk
        (
            key UInt64,
            meta UInt32,
            s    AggregateFunction(topK(20), String)
        )
        ENGINE = MergeTree
        ORDER BY key
        SETTINGS min_bytes_for_wide_part = 999999999
        """
    )

    # Initial state on disk has alpha_size = nextAlphaSize(60) = 512
    # (clean — the INSERT path goes through `add` which sizes alpha_map
    # correctly).
    node.query(
        """
        INSERT INTO t_topk
        SELECT 1, 0, topKState(20)(toString(number))
        FROM numbers(20)
        """
    )

    parts_before_alter = node.query(
        "SELECT name, part_type FROM system.parts WHERE table = 't_topk' AND active"
    ).strip()
    assert "Compact" in parts_before_alter, (
        f"expected a compact part, got {parts_before_alter!r}"
    )

    # ALTER UPDATE on `meta` rewrites every column of the compact part. The
    # `s` column is read into a `ColumnAggregateFunction` (each state is
    # deserialized — the buggy `readAlphaMap` grows alpha_map by
    # nextAlphaSize(min(20 + 1, 60)) = 128, leaving alpha_map.size() = 640
    # in memory) and serialized back into the new part as-is. No
    # `IAggregateFunction::merge` runs in this single-source path, so
    # `set.resize(reserved)` never restores the size before write.
    node.query(
        "ALTER TABLE t_topk UPDATE meta = 1 WHERE 1=1",
        settings={"mutations_sync": 2},
    )


def _read_back(node):
    return node.query(
        "SELECT length(topKMerge(20)(s)) FROM t_topk GROUP BY key ORDER BY key"
    ).strip()


def test_topk_state_can_be_read_after_upgrade(started_cluster):
    _create_and_corrupt(node)

    # Sanity: the buggy version itself reads the corrupted state without
    # error (no strict check yet on this version).
    assert _read_back(node) == "20"

    node.restart_with_latest_version()

    # On the upgraded binary the on-disk `alpha_size` is 640 while the
    # reader expects nextAlphaSize(60) = 512. Without the compatibility
    # fix this throws SIZES_OF_ARRAYS_DONT_MATCH.
    assert _read_back(node) == "20"

    # Self-healing: a fresh mutation on the upgraded binary must succeed
    # and re-serialize the state with a clean `alpha_size = 512`.
    node.query(
        "ALTER TABLE t_topk UPDATE meta = 2 WHERE 1=1",
        settings={"mutations_sync": 2},
    )
    assert _read_back(node) == "20"

    node.query("DROP TABLE t_topk SYNC;")
    node.restart_with_original_version()
