#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `parallel_replicas_allow_view_over_mergetree = 1` reads the `MergeTree` table below a "simple"
# view with parallel replicas, shipping the whole outer query - here the aggregation - to the
# replicas. For a `SQL SECURITY DEFINER` / `NONE` view the shortcut must be declined when an
# `additional_table_filters` entry of the invoker applies to the view (05105): a replica would
# then read the view through `StorageView::readImpl` with no coordination and return every row
# once per replica. An entry keyed to an unrelated table cannot make a replica take that path, so
# a projection-only barrier view keeps the shortcut with it, exactly like its `SQL SECURITY
# INVOKER` twin. The oracle is whether the aggregation is merged on the initiator
# (`MergingAggregated`: the replicas aggregated) or computed there (`Aggregating` over the rows
# the view's own inner query fetched with parallel replicas, which is what a declined shortcut
# falls back to); the three views must agree. With `serialize_query_plan = 0` the planner takes
# any query with an additional table filter off the parallel-replicas path regardless of the
# view, so only the serialized-plan round shows the shortcut. The `rows` lines are the correctness
# oracle: the unrelated entry filters nothing, all 6 rows come back once.

db=${CLICKHOUSE_DATABASE}
invoker="user05213_${CLICKHOUSE_DATABASE}_$RANDOM"
definer="definer05213_${CLICKHOUSE_DATABASE}_$RANDOM"

${CLICKHOUSE_CLIENT} <<EOSQL
CREATE TABLE $db.uatf_secrets (owner String, secret String) ENGINE = MergeTree ORDER BY owner;
INSERT INTO $db.uatf_secrets SELECT 'visible_owner', 'visible_' || toString(number) FROM numbers(3);
INSERT INTO $db.uatf_secrets SELECT 'someone_else', 'other_' || toString(number) FROM numbers(3);

CREATE TABLE $db.uatf_unrelated (owner String) ENGINE = MergeTree ORDER BY owner;
INSERT INTO $db.uatf_unrelated VALUES ('visible_owner');

CREATE USER $invoker;
CREATE USER $definer;
GRANT SELECT ON $db.uatf_secrets TO $definer;
GRANT SELECT ON $db.uatf_secrets TO $invoker;
GRANT SELECT ON $db.uatf_unrelated TO $invoker;
GRANT CREATE TEMPORARY TABLE ON *.* TO $invoker;

-- Projection-only views: they hide no row of the source table by themselves.
CREATE VIEW $db.uatf_definer_view DEFINER = $definer SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.uatf_secrets;

CREATE VIEW $db.uatf_none_view SQL SECURITY NONE
AS SELECT owner, secret FROM $db.uatf_secrets;

CREATE VIEW $db.uatf_invoker_view SQL SECURITY INVOKER
AS SELECT owner, secret FROM $db.uatf_secrets;

GRANT SELECT ON $db.uatf_definer_view TO $invoker;
GRANT SELECT ON $db.uatf_none_view TO $invoker;
GRANT SELECT ON $db.uatf_invoker_view TO $invoker;
EOSQL

PR_SETTINGS="--enable_analyzer 1 --enable_parallel_replicas 1 --max_parallel_replicas 3 \
    --cluster_for_parallel_replicas test_cluster_one_shard_three_replicas_localhost \
    --parallel_replicas_for_non_replicated_merge_tree 1 --parallel_replicas_plan_based 0 \
    --parallel_replicas_allow_view_over_mergetree 1 --parallel_replicas_local_plan 0 \
    --parallel_replicas_min_number_of_rows_per_replica 0 --automatic_parallel_replicas_mode 0"

function aggregation_on_replicas()
{
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} $PR_SETTINGS --serialize_query_plan "$1" --user "$invoker" --query "
        SELECT countIf(explain LIKE '%MergingAggregated%')
        FROM (EXPLAIN indexes = 0 SELECT count() FROM $2)
        FORMAT TSV"
}

function rows()
{
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} $PR_SETTINGS --serialize_query_plan "$1" --user "$invoker" --query "$2"
}

for serialize in 0 1; do
    for view in uatf_definer_view uatf_none_view uatf_invoker_view; do
        echo "--- $view (serialize_query_plan = $serialize) ---"
        echo -e "no filter, aggregation on the replicas:\t$(aggregation_on_replicas "$serialize" "$db.$view")"
        echo -e "filter on an unrelated table, aggregation on the replicas:\t$(aggregation_on_replicas "$serialize" "$db.$view
            SETTINGS additional_table_filters = {'$db.uatf_unrelated': 'owner = ''nobody'''}")"
        echo -e "filter on an unrelated bare name, aggregation on the replicas:\t$(aggregation_on_replicas "$serialize" "$db.$view
            SETTINGS additional_table_filters = {'uatf_unrelated': 'owner = ''nobody'''}")"
        # The controls: an entry that does apply to the barrier view, by name or by alias, still
        # declines the shortcut (the INVOKER twin has no such rule, see 05105).
        if [[ $view != uatf_invoker_view ]]; then
            echo -e "filter on the view, aggregation on the replicas:\t$(aggregation_on_replicas "$serialize" "$db.$view
                SETTINGS additional_table_filters = {'$db.$view': 'owner = ''visible_owner'''}")"
            echo -e "filter on the alias, aggregation on the replicas:\t$(aggregation_on_replicas "$serialize" "$db.$view AS uatf_alias
                SETTINGS additional_table_filters = {'uatf_alias': 'owner = ''visible_owner'''}")"
        fi
        echo -e "rows with the filter on an unrelated table:\t$(rows "$serialize" "
            SELECT count() FROM $db.$view
            SETTINGS additional_table_filters = {'$db.uatf_unrelated': 'owner = ''nobody'''}")"
        echo -e "rows with the filter on an unrelated bare name:\t$(rows "$serialize" "
            SELECT count() FROM $db.$view
            SETTINGS additional_table_filters = {'uatf_unrelated': 'owner = ''nobody'''}")"
    done
done

${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.uatf_definer_view, $db.uatf_none_view, $db.uatf_invoker_view"
${CLICKHOUSE_CLIENT} --query "DROP USER $invoker, $definer"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.uatf_secrets, $db.uatf_unrelated"
