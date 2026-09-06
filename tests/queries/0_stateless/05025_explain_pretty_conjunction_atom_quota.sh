#!/usr/bin/env bash
# Tags: long, no-tsan, no-flaky-check
# Random settings limits: min_bytes_for_full_part_storage=(0, 0)
#
# A wide part written into one packed file costs time in the number of columns, and these tables
# are thousands of columns wide. The clamp holds packed storage off, which is also its default.
#
# A conjunction can hold a user condition and runtime filter atoms at once, and the two render as
# separate output lines under separate length budgets.
#
# Crossing the walk's bound through the condition class takes two join keys, so this arm reaches it
# without the join planning that grows quadratically in key count. The arms that reach it through
# runtime filters need one key per filter and live in 05031 and 05032. no-flaky-check because
# repeating this cannot expose anything the single run does not.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A runtime filter only reaches the source filter under the analyzer, so without it these queries
# have nothing to render. It is rejected inside a query's SETTINGS clause, hence the client flag.
CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --enable_analyzer 1"

# Both prewhere settings are pinned because either one off leaves the condition in a Filter step,
# where no runtime filter joins it and the query selects no rows at all.
join_settings="enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0,
               join_algorithm = 'parallel_hash', parallel_hash_join_threshold = 0,
               query_plan_join_swap_table = 0, enable_parallel_replicas = 0, use_statistics = 0,
               query_plan_optimize_join_order_limit = 0, query_plan_join_shard_by_pk_ranges = 0,
               query_plan_optimize_join_order_randomize = 0, optimize_move_to_prewhere = 1,
               query_plan_optimize_prewhere = 1, allow_reorder_prewhere_conditions = 0"

# The mirror ordering: the class the walk collected in full is the runtime filter one, and the class
# it left outstanding is the condition. Which line carries the marker has to follow what the walk
# actually left behind rather than the fact that it stopped, so a class collected completely keeps no
# marker while the other is outstanding. Two join keys keep the runtime filter class small enough to
# finish, and one distinct condition per column overflows the other: a condition repeated on one
# column is rewritten into a single `NOT IN` and a shared alias chain collapses to one atom, so
# neither leaves anything outstanding. The condition count is above the walk's visit bound rather
# than the per-class quota, because reaching the quota alone still lets the walk go on to drain the
# rest.
M=8300
conds=$(seq 0 $((M - 1)) | awk '{printf "%su%s != %s", (NR > 1 ? " AND " : ""), $1, $1}')
cols=$(seq 0 $((M - 1)) | sed 's/^/u/;s/$/ UInt64/' | paste -sd,)
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS r1_05025"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS r2_05025"
# Piped rather than passed as an argument: one argv entry is capped well below this length.
echo "CREATE TABLE r1_05025 (k0 UInt64, k1 UInt64, $cols) ENGINE = MergeTree ORDER BY k0" \
    | $CLICKHOUSE_CLIENT --max_query_size 20000000 --max_ast_elements 20000000 --max_expanded_ast_elements 20000000
$CLICKHOUSE_CLIENT -q "CREATE TABLE r2_05025 (k0 UInt64, k1 UInt64) ENGINE = MergeTree ORDER BY k0"
# Only the join keys are given values. The conjunction is rendered from the query, not from the data,
# so the condition columns need to exist rather than to hold anything; one value expression per
# column would cost more to evaluate than the rest of the test.
$CLICKHOUSE_CLIENT -q "INSERT INTO r1_05025 (k0, k1) SELECT number, number FROM numbers(10)"
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
