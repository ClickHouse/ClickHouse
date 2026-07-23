#!/usr/bin/env bash
# Tags: long, no-parallel-replicas
# no-parallel-replicas: direct JOIN over a MergeTree right table is a single-node lookup.

# Regression test for a use-of-uninitialized-value (found by the AST fuzzer under MSan) in a
# `direct` JOIN whose right MergeTree table is read with the `STREAM` keyword.
#
# The direct-join lookup (DirectJoinMergeTreeEntity) rebuilds its lookup pipeline repeatedly over
# a StorageSnapshot shared with every clone of that plan. The strip-parts optimization in
# ReadFromMergeTree::initializePipeline used to mutate that shared snapshot in place
# (`storage_snapshot->data = std::move(...)`), so one pipeline build destroyed a SnapshotData
# whose `mutations_snapshot` shared_ptr was still read by an overlapping build. Projecting a
# column added by ALTER ... ADD COLUMN drives the `mutations_snapshot` path.
#
# The crash is scheduling-dependent (independent of max_threads), so we spawn the streaming query
# several times and assert the server never dies. Before the fix the server aborts within a
# handful of iterations under MSan.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_djs_left"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_djs_right"

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_djs_left (id UInt64) ENGINE = MergeTree ORDER BY id"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_djs_right (id UInt64, value String) ENGINE = MergeTree ORDER BY id"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_djs_right VALUES (1, 'a')"
# One matching left key (1) and one non-matching (99), each its own part, so the lookup takes
# both the found and the not-found (default-filling) paths across separate blocks.
$CLICKHOUSE_CLIENT -q "INSERT INTO t_djs_left VALUES (1)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_djs_left VALUES (99)"
# new_col is not physically present in the pre-ALTER right part.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_djs_right ADD COLUMN new_col String DEFAULT 'default_value'"

# The bug only exists on the direct-join path; confirm the query actually uses it (and would fail
# loudly here if planning changed and it silently stopped exercising the fixed code).
$CLICKHOUSE_CLIENT --enable_analyzer=1 --join_algorithm=direct -q "
    SELECT countIf(explain LIKE '%DirectKeyValueJoin%') > 0
    FROM (EXPLAIN PLAN SELECT l.id, r.value, r.new_col FROM t_djs_left AS l INNER JOIN t_djs_right AS r ON l.id = r.id)"

# A STREAM read never terminates on its own, so run it in the background and kill it each time.
# enable_analyzer=1 and join_algorithm=direct are pinned (they are the contract, CI randomizes
# enable_analyzer); max_block_size=1 rebuilds the lookup pipeline per left block, maximising the
# overlap that triggered the bug.
query="SELECT l.id, r.value, r.new_col FROM t_djs_left AS l INNER JOIN t_djs_right AS r STREAM ON l.id = r.id"
for _ in {1..20}; do
    timeout 3 $CLICKHOUSE_CLIENT --enable_streaming_queries=1 --enable_analyzer=1 --join_algorithm=direct --max_block_size=1 -q "$query" >/dev/null 2>&1 &
    bg_pid=$!
    sleep 1
    kill "$bg_pid" 2>/dev/null
    wait "$bg_pid" 2>/dev/null
    # If the server died the next query fails; surface it instead of the expected "alive".
    $CLICKHOUSE_CLIENT -q "SELECT 1" >/dev/null 2>&1 || { echo "server died"; break; }
done

echo "alive"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_djs_left"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_djs_right"
