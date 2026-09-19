#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The query text the analyzer ships to the replicas of `parallel_replicas_allow_view_over_mergetree`
# names every table expression `__table<N>`, so an `additional_table_filters` entry keyed to such a
# name applies to the view there and makes a `SQL SECURITY DEFINER` / `NONE` view decline the shortcut
# (05213). The check must match exactly `__table` followed by digits: a user-visible table that merely
# starts with `__table`, like `__table_prod`, can never be synthesized by the planner, so an entry keyed
# to it is an unrelated one and must leave a projection-only barrier view as optimizable as its
# `SQL SECURITY INVOKER` twin. The oracle is the same as in 05213: `MergingAggregated` means the
# replicas aggregated (the shortcut was taken), and it is visible with `serialize_query_plan = 1` only.

db=${CLICKHOUSE_DATABASE}
invoker="user05215_${CLICKHOUSE_DATABASE}_$RANDOM"
definer="definer05215_${CLICKHOUSE_DATABASE}_$RANDOM"

${CLICKHOUSE_CLIENT} <<EOSQL
CREATE TABLE $db.utpf_secrets (owner String, secret String) ENGINE = MergeTree ORDER BY owner;
INSERT INTO $db.utpf_secrets SELECT 'visible_owner', 'visible_' || toString(number) FROM numbers(3);
INSERT INTO $db.utpf_secrets SELECT 'someone_else', 'other_' || toString(number) FROM numbers(3);

CREATE TABLE $db.__table_prod (owner String) ENGINE = MergeTree ORDER BY owner;
INSERT INTO $db.__table_prod VALUES ('visible_owner');

CREATE USER $invoker;
CREATE USER $definer;
GRANT SELECT ON $db.utpf_secrets TO $definer;
GRANT SELECT ON $db.utpf_secrets TO $invoker;
GRANT SELECT ON $db.__table_prod TO $invoker;
GRANT CREATE TEMPORARY TABLE ON *.* TO $invoker;

-- Projection-only views: they hide no row of the source table by themselves.
CREATE VIEW $db.utpf_definer_view DEFINER = $definer SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.utpf_secrets;

CREATE VIEW $db.utpf_none_view SQL SECURITY NONE
AS SELECT owner, secret FROM $db.utpf_secrets;

CREATE VIEW $db.utpf_invoker_view SQL SECURITY INVOKER
AS SELECT owner, secret FROM $db.utpf_secrets;

GRANT SELECT ON $db.utpf_definer_view TO $invoker;
GRANT SELECT ON $db.utpf_none_view TO $invoker;
GRANT SELECT ON $db.utpf_invoker_view TO $invoker;
EOSQL

PR_SETTINGS="--enable_analyzer 1 --enable_parallel_replicas 1 --max_parallel_replicas 3 \
    --cluster_for_parallel_replicas test_cluster_one_shard_three_replicas_localhost \
    --parallel_replicas_for_non_replicated_merge_tree 1 --parallel_replicas_plan_based 0 \
    --parallel_replicas_allow_view_over_mergetree 1 --parallel_replicas_local_plan 0 \
    --parallel_replicas_min_number_of_rows_per_replica 0 --automatic_parallel_replicas_mode 0 \
    --serialize_query_plan 1"

function aggregation_on_replicas()
{
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} $PR_SETTINGS --user "$invoker" --query "
        SELECT countIf(explain LIKE '%MergingAggregated%')
        FROM (EXPLAIN indexes = 0 SELECT count() FROM $1)
        FORMAT TSV"
}

function rows()
{
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} $PR_SETTINGS --user "$invoker" --query "$1"
}

for view in utpf_definer_view utpf_none_view utpf_invoker_view; do
    echo "--- $view ---"
    echo -e "no filter, aggregation on the replicas:\t$(aggregation_on_replicas "$db.$view")"
    echo -e "filter on a bare user table named like an internal alias, aggregation on the replicas:\t$(aggregation_on_replicas "$db.$view
        SETTINGS additional_table_filters = {'__table_prod': 'owner = ''nobody'''}")"
    echo -e "filter on a qualified user table named like an internal alias, aggregation on the replicas:\t$(aggregation_on_replicas "$db.$view
        SETTINGS additional_table_filters = {'$db.__table_prod': 'owner = ''nobody'''}")"
    # The control: an entry keyed to an internal alias proper does apply to the view on a replica,
    # so a barrier view declines the shortcut for it (the INVOKER twin has no such rule, see 05105).
    if [[ $view != utpf_invoker_view ]]; then
        echo -e "filter on an internal alias, aggregation on the replicas:\t$(aggregation_on_replicas "$db.$view
            SETTINGS additional_table_filters = {'__table1': 'owner = ''visible_owner'''}")"
    fi
    echo -e "rows with the filter on a bare user table named like an internal alias:\t$(rows "
        SELECT count() FROM $db.$view
        SETTINGS additional_table_filters = {'__table_prod': 'owner = ''nobody'''}")"
done

${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.utpf_definer_view, $db.utpf_none_view, $db.utpf_invoker_view"
${CLICKHOUSE_CLIENT} --query "DROP USER $invoker, $definer"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.utpf_secrets, $db.__table_prod"
