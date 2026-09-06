#!/usr/bin/env bash
# Tags: long, no-tsan, no-msan, no-flaky-check
# Random settings limits: min_bytes_for_full_part_storage=(0, 0)
#
# A wide part written into one packed file costs time in the number of columns, and these tables
# are thousands of columns wide. The clamp holds packed storage off, which is also its default.
#
# Crossing the per-class atom quota with runtime filters takes one join key per filter, so the joins
# are generated here rather than written out. One runtime filter per join key is also one nested plan
# node per join key, so a key count is a plan depth, and this arm is what covers the walk over that
# depth on an instrumented build, where a frame costs more than the depth a plain build reaches.
#
# Planning a join grows quadratically in its key count and is most of the run time, of which
# rendering is a fraction of a second. That planning is instrumented along with everything else, so
# the slowest instrumentation exceeds the per-test cap and is excluded; the same quota is crossed
# through the condition class in 05025, which no build skips. no-flaky-check because repeating this
# cannot expose anything the single run does not.

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

# One atom past the 4096 quota, so the condition and the annotation compete for it. The annotation is
# asserted at its exact capped length, with the `Runtime filters: ` label stripped: an upper bound is
# also satisfied by a short marked replacement, and the marker must trail this same line rather than
# some other emitter's.
N=4097
cols=$(seq 0 $((N - 1)) | sed 's/^/k/;s/$/ UInt64/' | paste -sd,)
using=$(seq 0 $((N - 1)) | sed 's/^/k/' | paste -sd,)
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS q1_05032"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS q2_05032"
# Piped rather than passed as an argument: one argv entry is capped well below these lengths.
echo "CREATE TABLE q1_05032 (a UInt64, b String, $cols) ENGINE = MergeTree ORDER BY a" \
    | $CLICKHOUSE_CLIENT --max_query_size 4000000 --max_ast_elements 4000000 --max_expanded_ast_elements 4000000
echo "CREATE TABLE q2_05032 ($cols) ENGINE = MergeTree ORDER BY k0" \
    | $CLICKHOUSE_CLIENT --max_query_size 4000000 --max_ast_elements 4000000 --max_expanded_ast_elements 4000000
# A runtime filter is built per join key whether or not the key column holds anything, and the tables
# have to be non-empty for one to reach the source filter at all. Only the columns the assertions read
# are given values: one value expression per key column would cost more to evaluate than the query
# under test.
$CLICKHOUSE_CLIENT -q "INSERT INTO q1_05032 (a, b) SELECT number, toString(number) FROM numbers(10)"
$CLICKHOUSE_CLIENT -q "INSERT INTO q2_05032 (k0) SELECT number FROM numbers(10)"
$CLICKHOUSE_CLIENT --max_query_size 1000000 -q "
SELECT countIf(explain LIKE '%Prewhere filter column:%' AND explain LIKE '%b != %') > 0,
       countIf(explain LIKE '%Runtime filters: RF%') > 0,
       max(length(extract(explain, '^[^A-Za-z]*Runtime filters: (.*)\$'))) = 8178,
       countIf(explain LIKE '%Runtime filters:%' AND explain LIKE '%...') > 0
FROM (EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
      SELECT q1_05032.a FROM q1_05032 INNER JOIN q2_05032 USING ($using) WHERE b != 'q'
      SETTINGS $join_settings)"

$CLICKHOUSE_CLIENT -q "DROP TABLE q1_05032"
$CLICKHOUSE_CLIENT -q "DROP TABLE q2_05032"
