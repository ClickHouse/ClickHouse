#!/usr/bin/env bash
# Tags: shard

# Global ORDER BY must be applied across shards when a Distributed table is read through a
# Merge engine. Before the fix, ReadFromMerge narrowed (concatenated) the per-shard-sorted
# streams at max_threads = 1, so the outer merge-only sort had a single input and LIMIT
# returned one shard's local top rows instead of the global top rows (issue #111211).
#
# Parallel-safe: the two shard tables get a per-test unique name in the shared shard_0/shard_1
# databases, and the Distributed/Merge tables live in the per-test database. The shared
# databases are only created (never dropped), so concurrent copies of this test do not collide.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

t="t_${CLICKHOUSE_DATABASE}"
t2="t2_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
CREATE DATABASE IF NOT EXISTS shard_0;
CREATE DATABASE IF NOT EXISTS shard_1;
DROP TABLE IF EXISTS shard_0.${t} SYNC;
DROP TABLE IF EXISTS shard_1.${t} SYNC;
DROP TABLE IF EXISTS shard_0.${t2} SYNC;
DROP TABLE IF EXISTS shard_1.${t2} SYNC;
CREATE TABLE shard_0.${t} (w Int64) ENGINE = MergeTree ORDER BY w;
CREATE TABLE shard_1.${t} (w Int64) ENGINE = MergeTree ORDER BY w;
INSERT INTO shard_0.${t} SELECT number * 3 FROM numbers(100);        -- max 297
INSERT INTO shard_1.${t} SELECT number * 3 + 1000 FROM numbers(100); -- max 1297
CREATE TABLE dist AS shard_0.${t} ENGINE = Distributed(test_cluster_two_shards_different_databases, '', ${t});
CREATE TABLE merge_t AS dist ENGINE = Merge(currentDatabase(), '^dist\$');
-- Second Distributed table, disjoint higher range, for a Merge that matches two children.
CREATE TABLE shard_0.${t2} (w Int64) ENGINE = MergeTree ORDER BY w;
CREATE TABLE shard_1.${t2} (w Int64) ENGINE = MergeTree ORDER BY w;
INSERT INTO shard_0.${t2} SELECT number * 3 + 5000 FROM numbers(100); -- max 5297
INSERT INTO shard_1.${t2} SELECT number * 3 + 9000 FROM numbers(100); -- max 9297
CREATE TABLE dist2 AS shard_0.${t2} ENGINE = Distributed(test_cluster_two_shards_different_databases, '', ${t2});
CREATE TABLE merge_multi AS dist ENGINE = Merge(currentDatabase(), '^dist');
"

# max_threads = 1 is what makes narrowPipe collapse the two per-shard streams into one.
run() { ${CLICKHOUSE_CLIENT} --max_threads 1 --query "$1"; }
# Count the rows a bare query emits to the client. Wrapping in SELECT count() would let the
# optimizer drop the inner ORDER BY (order is irrelevant to count), which hides the bug; the
# ORDER BY only executes on the client-facing bare query, so we count its output rows.
rowcount() { ${CLICKHOUSE_CLIENT} --max_threads 1 --query "$1" | wc -l | tr -d ' '; }

echo 'analyzer, DESC'
run "SELECT w FROM merge_t ORDER BY w DESC LIMIT 3 SETTINGS enable_analyzer = 1"
echo 'analyzer, ASC'
run "SELECT w FROM merge_t ORDER BY w ASC LIMIT 3 SETTINGS enable_analyzer = 1"
echo 'old analyzer, DESC'
run "SELECT w FROM merge_t ORDER BY w DESC LIMIT 3 SETTINGS enable_analyzer = 0"

# After-aggregation stages also use a merge-only sort.
echo 'analyzer, group by + DESC, no_merge=2'
run "SELECT w FROM merge_t GROUP BY w ORDER BY w DESC LIMIT 3 SETTINGS enable_analyzer = 1, distributed_group_by_no_merge = 2"
echo 'old analyzer, group by + DESC, no_merge=2'
run "SELECT w FROM merge_t GROUP BY w ORDER BY w DESC LIMIT 3 SETTINGS enable_analyzer = 0, distributed_group_by_no_merge = 2"
echo 'analyzer, group by + DESC, no_merge=2, push_down_limit=0'
run "SELECT w FROM merge_t GROUP BY w ORDER BY w DESC LIMIT 3 SETTINGS enable_analyzer = 1, distributed_group_by_no_merge = 2, distributed_push_down_limit = 0"

# Order-sensitive head/tail: the top rows come from shard 1, the bottom from shard 0,
# so a broken global sort changes these boundary rows (min/max/count would not).
echo 'full order head'
run "SELECT * FROM (SELECT w FROM merge_t ORDER BY w DESC LIMIT 2) SETTINGS enable_analyzer = 1"
echo 'full order tail'
run "SELECT * FROM (SELECT w FROM merge_t ORDER BY w ASC LIMIT 2) SETTINGS enable_analyzer = 1"

# Unbounded ORDER BY: no LIMIT means the SortingStep keeps no sort limit, a distinct code path.
# 1 = the whole result is globally descending; a per-shard concatenation would break it because
# shard 0 (max 297) sorts before shard 1 (min 1000) under narrowPipe.
echo 'no limit, monotonic DESC, analyzer'
run "SELECT arraySort(x -> -x, groupArray(w)) = groupArray(w) FROM (SELECT w FROM merge_t ORDER BY w DESC) SETTINGS enable_analyzer = 1"
echo 'no limit, monotonic DESC, old analyzer'
run "SELECT arraySort(x -> -x, groupArray(w)) = groupArray(w) FROM (SELECT w FROM merge_t ORDER BY w DESC) SETTINGS enable_analyzer = 0"

# Two matching children cap the stage at WithMergeableState (not after-aggregation), and
# distributed_aggregation_memory_efficient = 0 disables the older guard, so only the
# WithMergeableState arm keeps narrowPipe from concatenating the sorted shard streams.
# 1 = the whole result is globally descending.
echo 'no limit, two children, monotonic DESC, analyzer'
run "SELECT arraySort(x -> -x, groupArray(w)) = groupArray(w) FROM (SELECT w FROM merge_multi ORDER BY w DESC) SETTINGS enable_analyzer = 1, distributed_aggregation_memory_efficient = 0"
echo 'no limit, two children, monotonic DESC, old analyzer'
run "SELECT arraySort(x -> -x, groupArray(w)) = groupArray(w) FROM (SELECT w FROM merge_multi ORDER BY w DESC) SETTINGS enable_analyzer = 0, distributed_aggregation_memory_efficient = 0"

# DISTINCT (issue #111211): the same narrowPipe concatenation makes the result MULTISET
# wrong, not just the order. Both shards of test_cluster_two_shards_localhost read the same
# table, so DISTINCT must dedup back to 21; without the fix the second shard's sorted run
# survives adjacent-only dedup and the query returns 42. Count the bare query's output rows
# (rowcount), not SELECT count() over it: count() lets the optimizer drop the inner ORDER BY,
# hiding the bug.
#   - distributed_aggregation_memory_efficient = 0 is required, else the older should_not_narrow
#     guard already hides it at WithMergeableState.
#   - optimize_distinct_in_order = 1 is required to keep the case discriminating: with it 0,
#     DistinctStep hash-dedups the whole stream and returns 21 even when narrowing corrupts the
#     order, so an unfixed build would still pass. The runner randomizes this setting.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE td (s String) ENGINE = MergeTree ORDER BY s;
INSERT INTO td SELECT toString(number % 21) FROM numbers(100);
CREATE TABLE dd AS td ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), td);
CREATE TABLE md AS dd ENGINE = Merge(currentDatabase(), '^dd\$');
"

echo 'distinct, analyzer'
rowcount "SELECT DISTINCT s FROM md ORDER BY s SETTINGS enable_analyzer = 1, distributed_aggregation_memory_efficient = 0, optimize_distinct_in_order = 1"
echo 'distinct, old analyzer'
rowcount "SELECT DISTINCT s FROM md ORDER BY s SETTINGS enable_analyzer = 0, distributed_aggregation_memory_efficient = 0, optimize_distinct_in_order = 1"
echo 'distinct on, analyzer'
rowcount "SELECT DISTINCT ON (s) s FROM md ORDER BY s SETTINGS enable_analyzer = 1, distributed_aggregation_memory_efficient = 0, optimize_distinct_in_order = 1"

${CLICKHOUSE_CLIENT} --query "
DROP TABLE md;
DROP TABLE dd;
DROP TABLE td;
DROP TABLE merge_multi;
DROP TABLE merge_t;
DROP TABLE dist;
DROP TABLE dist2;
DROP TABLE shard_0.${t} SYNC;
DROP TABLE shard_1.${t} SYNC;
DROP TABLE shard_0.${t2} SYNC;
DROP TABLE shard_1.${t2} SYNC;
"
