#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Plan-based parallel replicas ship a plan fragment, not SQL text. `collectReadsToDistribute` refuses
# to distribute anything below a security barrier, but that only covers the coordinated side of a
# join: once `liftSplitAboveJoin` pulls the split above the join, the whole join subtree becomes the
# fragment and the broadcast side rides along, so a `SQL SECURITY DEFINER` / `NONE` view sitting on
# the non-coordinated side would be executed on every replica under the connection's identity.
#
# The oracle is the root step of the plan. When the join ships, the split is above it and the plan
# root is the `Union` of the local plan and the remote read; when it does not, the join stays local
# and the `Union` sits below it, on the coordinated side only. `LEFT` coordinates the left side and
# `RIGHT` the right one, so both shapes put the view on the broadcast side.

db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOSQL
CREATE TABLE $db.jb_secrets (k UInt64, secret String) ENGINE = MergeTree ORDER BY k;
INSERT INTO $db.jb_secrets SELECT number, if(number < 3, 'visible_', 'HIDDEN_') || toString(number) FROM numbers(6);

CREATE TABLE $db.jb_plain (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO $db.jb_plain SELECT number, 'v' || toString(number) FROM numbers(6);

CREATE VIEW $db.jb_none_view SQL SECURITY NONE
AS SELECT k, secret FROM $db.jb_secrets WHERE k < 3;

CREATE VIEW $db.jb_invoker_view SQL SECURITY INVOKER
AS SELECT k, secret FROM $db.jb_secrets WHERE k < 3;
EOSQL

PR_SETTINGS="--enable_analyzer 1 --enable_parallel_replicas 1 --max_parallel_replicas 3 \
    --cluster_for_parallel_replicas test_cluster_one_shard_three_replicas_localhost \
    --parallel_replicas_for_non_replicated_merge_tree 1 --parallel_replicas_plan_based 1 \
    --parallel_replicas_local_plan 1 --parallel_replicas_min_number_of_rows_per_replica 0 \
    --automatic_parallel_replicas_mode 0"

function plan_shape()
{
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} $PR_SETTINGS --query "
        SELECT countIf(explain LIKE 'Union%') AS shipped_join, countIf(explain LIKE 'Join%') AS local_join
        FROM (EXPLAIN optimize = 1, description = 0 $1)"
}

for view in jb_none_view jb_invoker_view; do
    echo "--- $view ---"
    echo "left, view on the broadcast side (shipped join / local join): $(plan_shape "SELECT p.v, x.secret FROM $db.jb_plain AS p LEFT JOIN $db.$view AS x ON p.k = x.k")"
    echo "right, view on the broadcast side (shipped join / local join): $(plan_shape "SELECT p.v, x.secret FROM $db.$view AS x RIGHT JOIN $db.jb_plain AS p ON p.k = x.k")"
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} $PR_SETTINGS --query \
        "SELECT p.v, x.secret FROM $db.jb_plain AS p LEFT JOIN $db.$view AS x ON p.k = x.k ORDER BY ALL"
done

${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.jb_none_view, $db.jb_invoker_view"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.jb_secrets, $db.jb_plain"
