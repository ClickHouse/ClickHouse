#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A join runtime filter carries the join keys of one side into the read of the other side. Across the
# seal of a `SQL SECURITY DEFINER` / `SQL SECURITY NONE` view that is both a disclosure channel (how
# many rows the view reads starts to depend on the invoker's expressions, and the keys of rows the
# view hides leave the seal) and a wrong-result hazard: the seal keeps the view's read local while
# the other side is read by parallel replicas, so the filter is built from the coordinated side's own
# share of the ranges only and drops every row whose key was assigned to another replica.
#
# The oracle is whether the optimized plan carries an `Apply runtime join filter` step, plus the rows
# themselves. `query_plan_join_swap_table = true` puts the view on the probe side, which is where the
# filter would be planted. The join order pass is pinned off as well (`query_plan_optimize_join_order_limit`,
# `query_plan_optimize_join_order_randomize`, `use_hash_table_stats_for_join_reordering`): with the
# settings randomizer it otherwise swaps the sides back and the positive control - the `SQL SECURITY
# INVOKER` twin, which must still get its filter - loses it. The oracle is a yes/no rather than a count
# because the number of steps is not stable either: with `query_plan_optimize_prewhere = 0` the same
# filter is applied by two steps instead of one.

db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOSQL
CREATE TABLE $db.rf_secrets (k UInt64, secret String) ENGINE = MergeTree ORDER BY k;
INSERT INTO $db.rf_secrets SELECT number, if(number < 3, 'visible_', 'HIDDEN_') || toString(number) FROM numbers(6);

CREATE TABLE $db.rf_plain (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO $db.rf_plain SELECT number, 'v' || toString(number) FROM numbers(6);

CREATE VIEW $db.rf_none_view SQL SECURITY NONE
AS SELECT k, secret FROM $db.rf_secrets WHERE k < 3;

CREATE VIEW $db.rf_invoker_view SQL SECURITY INVOKER
AS SELECT k, secret FROM $db.rf_secrets WHERE k < 3;
EOSQL

PR_SETTINGS="--enable_analyzer 1 --enable_parallel_replicas 1 --max_parallel_replicas 3 \
    --cluster_for_parallel_replicas test_cluster_one_shard_three_replicas_localhost \
    --parallel_replicas_for_non_replicated_merge_tree 1 --parallel_replicas_plan_based 1 \
    --parallel_replicas_local_plan 1 --parallel_replicas_min_number_of_rows_per_replica 0 \
    --automatic_parallel_replicas_mode 0 --max_threads 1 \
    --enable_join_runtime_filters 1 --join_runtime_filter_min_probe_rows 0 \
    --query_plan_join_swap_table true \
    --query_plan_optimize_join_order_randomize 0 --query_plan_optimize_join_order_limit 1 \
    --use_hash_table_stats_for_join_reordering 0"

for view in rf_none_view rf_invoker_view; do
    echo "--- $view ---"
    # shellcheck disable=SC2086
    echo "runtime filter applied: $(${CLICKHOUSE_CLIENT} $PR_SETTINGS --query "
        SELECT hasAny(groupArray(explain LIKE '%Apply runtime join filter%'), [true]) ? 'yes' : 'no'
        FROM (EXPLAIN optimize = 1 SELECT p.v, x.secret FROM $db.rf_plain AS p LEFT JOIN $db.$view AS x ON p.k = x.k)")"
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} $PR_SETTINGS --query \
        "SELECT p.v, x.secret FROM $db.rf_plain AS p LEFT JOIN $db.$view AS x ON p.k = x.k ORDER BY ALL"
done

${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.rf_none_view, $db.rf_invoker_view"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.rf_secrets, $db.rf_plain"
