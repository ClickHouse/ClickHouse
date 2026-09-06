#!/usr/bin/env bash
# Tags: long, no-sanitizers, no-flaky-check
# Random settings limits: min_bytes_for_full_part_storage=(0, 0)
#
# A wide part written into one packed file costs time in the number of columns, and these tables
# are thousands of columns wide. The clamp holds packed storage off, which is also its default.
#
# The walk over a conjunction stops after a bound on visits as well as on stored atoms per class, so
# a query holding one class only still terminates. Crossing that bound with runtime filters takes one
# join key per filter and so twice the keys of the per-class quota, and planning a join grows
# quadratically in its key count: these two arms are eight times the planning of the quota arm in
# 05032 while asserting the same rendering code, which is a fraction of a second of either. That
# planning is what no-sanitizers excludes, since it is instrumented along with everything else and
# there exceeds the per-test cap. The bound is reached on every other build, and reached far more
# cheaply through the condition class by 05025, which no build skips.
#
# no-flaky-check because repeating these cannot expose anything the single run does not.

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

make_join_tables() {
    local n=$1
    local cols
    cols=$(seq 0 $((n - 1)) | sed 's/^/k/;s/$/ UInt64/' | paste -sd,)
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS q1_05031"
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS q2_05031"
    # Piped rather than passed as an argument: one argv entry is capped well below these lengths.
    echo "CREATE TABLE q1_05031 (a UInt64, b String, $cols) ENGINE = MergeTree ORDER BY a" \
        | $CLICKHOUSE_CLIENT --max_query_size 4000000 --max_ast_elements 4000000 --max_expanded_ast_elements 4000000
    echo "CREATE TABLE q2_05031 ($cols) ENGINE = MergeTree ORDER BY k0" \
        | $CLICKHOUSE_CLIENT --max_query_size 4000000 --max_ast_elements 4000000 --max_expanded_ast_elements 4000000
    # A runtime filter is built per join key whether or not the key column holds anything, and the
    # tables have to be non-empty for one to reach the source filter at all. Only the columns the
    # assertions read are given values: one value expression per key column would cost more to
    # evaluate than the queries under test.
    $CLICKHOUSE_CLIENT -q "INSERT INTO q1_05031 (a, b) SELECT number, toString(number) FROM numbers(10)"
    $CLICKHOUSE_CLIENT -q "INSERT INTO q2_05031 (k0) SELECT number FROM numbers(10)"
}

# Enough keys that the walk over the conjunction runs out of visits before it reaches the condition:
# draining this conjunction costs one visit per runtime filter plus one for the enclosing `and` and
# one for the condition itself, and the visit bound is twice the per-class quota. The condition is the
# last node on the stack, so the walk ends having collected only runtime filters, and a class with
# nothing collected cannot tell "absent" from "not reached". The line must still be printed and marked
# partial: a line qualified with the marker says there is more, whereas no line at all says there is
# nothing. The rendered condition is also asserted at its exact length with the label stripped: the
# label and the marker are emitted whenever the fallback fires at all, so only the payload between
# them observes that the filter column is named.
N=8191
using=$(seq 0 $((N - 1)) | sed 's/^/k/' | paste -sd,)
make_join_tables $N
$CLICKHOUSE_CLIENT --max_query_size 1000000 -q "
SELECT countIf(explain LIKE '%Prewhere filter column:%') > 0,
       countIf(explain LIKE '%Prewhere filter column:%' AND explain LIKE '%...') > 0,
       max(length(extract(explain, '^[^A-Za-z]*Prewhere filter column: (.*)\$'))) = 8195
FROM (EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
      SELECT q1_05031.a FROM q1_05031 INNER JOIN q2_05031 USING ($using) WHERE b != 'q'
      SETTINGS $join_settings)"

# The same shape with no condition, so the conjunction holds runtime filters only. A source filter
# made purely of runtime filters renders as the annotation alone (see 04059_explain_pretty_filters),
# so naming a filter column here would report a condition the query does not have. Reading the stop
# alone as "a condition was missed" names one. One key past the visit bound rather than at it: the
# conjunction is a single `and` over one leaf per key, so at the bound exactly the walk drains and
# leaves nothing to classify.
N=8192
using=$(seq 0 $((N - 1)) | sed 's/^/k/' | paste -sd,)
make_join_tables $N
$CLICKHOUSE_CLIENT --max_query_size 1000000 -q "
SELECT countIf(explain LIKE '%Prewhere filter column:%'),
       countIf(explain LIKE '%Runtime filters: RF%') > 0
FROM (EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
      SELECT q1_05031.a FROM q1_05031 INNER JOIN q2_05031 USING ($using)
      SETTINGS $join_settings)"

$CLICKHOUSE_CLIENT -q "DROP TABLE q1_05031"
$CLICKHOUSE_CLIENT -q "DROP TABLE q2_05031"
