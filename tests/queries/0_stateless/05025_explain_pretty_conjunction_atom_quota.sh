#!/usr/bin/env bash
# Tags: long, no-tsan, no-flaky-check
#
# A conjunction can hold a user condition and runtime filter atoms at once, and the two render as
# separate output lines under separate length budgets. Crossing the per-class atom quota needs one
# runtime filter per join key, so the joins are generated here rather than written out.
#
# One runtime filter per join key also becomes one nested plan node per join key, so a key count is
# a plan depth, and this arm is what covers the walk over that depth. Planning a join grows
# quadratically in its key count and is most of the run time, of which rendering is a fraction of a
# second; the arms that cross the whole-walk visit bound need twice these keys and so eight times
# this planning, and live in 05031 apart from the sanitizer builds. no-flaky-check because
# repeating this cannot expose anything the single run does not.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A runtime filter only reaches the source filter under the analyzer, so without it these queries
# have nothing to render. It is rejected inside a query's SETTINGS clause, hence the client flag.
CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --enable_analyzer 1"

# Pinned settings, shared by both arms. Both prewhere settings are pinned because either one off
# leaves the condition in a Filter step, where no runtime filter joins it and the query selects no
# rows at all.
join_settings="enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0,
               join_algorithm = 'parallel_hash', parallel_hash_join_threshold = 0,
               query_plan_join_swap_table = 0, enable_parallel_replicas = 0, use_statistics = 0,
               query_plan_optimize_join_order_limit = 0, query_plan_join_shard_by_pk_ranges = 0,
               query_plan_optimize_join_order_randomize = 0, optimize_move_to_prewhere = 1,
               query_plan_optimize_prewhere = 1, allow_reorder_prewhere_conditions = 0"

make_join_tables() {
    local n=$1
    local cols
    cols=$(seq 0 $((n - 1)) | sed 's/^/k/;s/$/ UInt64/' | paste -sd,)
    local vals
    vals=$(seq 0 $((n - 1)) | sed 's/^/number+/' | paste -sd,)
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS q1_05025"
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS q2_05025"
    # Piped rather than passed as an argument: one argv entry is capped well below these lengths.
    echo "CREATE TABLE q1_05025 (a UInt64, b String, $cols) ENGINE = MergeTree ORDER BY a" \
        | $CLICKHOUSE_CLIENT --max_query_size 4000000 --max_ast_elements 4000000 --max_expanded_ast_elements 4000000
    echo "CREATE TABLE q2_05025 ($cols) ENGINE = MergeTree ORDER BY k0" \
        | $CLICKHOUSE_CLIENT --max_query_size 4000000 --max_ast_elements 4000000 --max_expanded_ast_elements 4000000
    echo "INSERT INTO q1_05025 SELECT number, toString(number), $vals FROM numbers(10)" \
        | $CLICKHOUSE_CLIENT --max_query_size 4000000 --max_ast_elements 4000000 --max_expanded_ast_elements 4000000
    echo "INSERT INTO q2_05025 SELECT $vals FROM numbers(10)" \
        | $CLICKHOUSE_CLIENT --max_query_size 4000000 --max_ast_elements 4000000 --max_expanded_ast_elements 4000000
}

# One atom past the 4096 quota, so the condition and the annotation compete for it. The annotation is
# asserted at its exact capped length, with the `Runtime filters: ` label stripped: an upper bound is
# also satisfied by a short marked replacement, and the marker must trail this same line rather than
# some other emitter's.
N=4097
using=$(seq 0 $((N - 1)) | sed 's/^/k/' | paste -sd,)
make_join_tables $N
$CLICKHOUSE_CLIENT --max_query_size 1000000 -q "
SELECT countIf(explain LIKE '%Prewhere filter column:%' AND explain LIKE '%b != %') > 0,
       countIf(explain LIKE '%Runtime filters: RF%') > 0,
       max(length(extract(explain, '^[^A-Za-z]*Runtime filters: (.*)\$'))) = 8178,
       countIf(explain LIKE '%Runtime filters:%' AND explain LIKE '%...') > 0
FROM (EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
      SELECT q1_05025.a FROM q1_05025 INNER JOIN q2_05025 USING ($using) WHERE b != 'q'
      SETTINGS $join_settings)"

$CLICKHOUSE_CLIENT -q "DROP TABLE q1_05025"
$CLICKHOUSE_CLIENT -q "DROP TABLE q2_05025"

# The mirror ordering: the class the walk collected in full is the runtime filter one, and the class
# it left outstanding is the condition. Which line carries the marker has to follow what the walk
# actually left behind rather than the fact that it stopped, so a class collected completely keeps no
# marker while the other is outstanding. Two join keys keep the runtime filter class small enough to
# finish, and one distinct condition per column overflows the other: a condition repeated on one
# column is rewritten into a single `NOT IN` and a shared alias chain collapses to one atom, so
# neither leaves anything outstanding. The condition count is above the walk's visit bound rather
# than the per-class quota, because reaching the quota alone still lets the walk go on to drain the
# rest. Conditions cross that bound at two join keys, so this arm reaches it without the planning
# cost that the key-count arms pay for the same bound.
M=8300
conds=$(seq 0 $((M - 1)) | awk '{printf "%su%s != %s", (NR > 1 ? " AND " : ""), $1, $1}')
cols=$(seq 0 $((M - 1)) | sed 's/^/u/;s/$/ UInt64/' | paste -sd,)
vals=$(seq 0 $((M - 1)) | sed 's/^/number+/' | paste -sd,)
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS r1_05025"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS r2_05025"
echo "CREATE TABLE r1_05025 (k0 UInt64, k1 UInt64, $cols) ENGINE = MergeTree ORDER BY k0" \
    | $CLICKHOUSE_CLIENT --max_query_size 20000000 --max_ast_elements 20000000 --max_expanded_ast_elements 20000000
$CLICKHOUSE_CLIENT -q "CREATE TABLE r2_05025 (k0 UInt64, k1 UInt64) ENGINE = MergeTree ORDER BY k0"
echo "INSERT INTO r1_05025 SELECT number, number, $vals FROM numbers(10)" \
    | $CLICKHOUSE_CLIENT --max_query_size 20000000 --max_ast_elements 20000000 --max_expanded_ast_elements 20000000
$CLICKHOUSE_CLIENT -q "INSERT INTO r2_05025 SELECT number, number FROM numbers(10)"
echo "
SELECT countIf(explain LIKE '%Prewhere filter column:%') > 0,
       countIf(explain LIKE '%Prewhere filter column:%' AND explain LIKE '%...') > 0,
       countIf(explain LIKE '%Runtime filters: RF%') > 0,
       countIf(explain LIKE '%Runtime filters:%' AND explain LIKE '%...'),
       max(length(extract(explain, '^[^A-Za-z]*Prewhere filter column: (.*)\$'))) = 8195
FROM (EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
      SELECT r1_05025.k0 FROM r1_05025 INNER JOIN r2_05025 USING (k0,k1) WHERE $conds
      SETTINGS $join_settings)" \
    | $CLICKHOUSE_CLIENT --max_query_size 20000000 --max_ast_elements 20000000 \
                         --max_expanded_ast_elements 20000000

$CLICKHOUSE_CLIENT -q "DROP TABLE r1_05025"
$CLICKHOUSE_CLIENT -q "DROP TABLE r2_05025"
