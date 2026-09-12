#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An `additional_table_filters` entry keyed to a view (by its qualified name or by its alias) is a
# predicate of the invoker, applied to the view's output. For a `SQL SECURITY DEFINER` / `NONE`
# view the barrier keeps the view a table expression instead of inlining it
# (`QueryAnalyzer::inlineViewSubqueryIfNeeded`), so the predicate becomes a separate `Filter` step
# above the view's own read.
#
# That must not combine with `parallel_replicas_allow_view_over_mergetree = 1`, which decides that
# a "simple" view can be read with parallel replicas because the `MergeTree` table below it can:
# the query the replicas receive still reads the view, a replica plans it on its own, and the
# barrier makes it read the view through `StorageView::readImpl`, which switches parallel replicas
# off for the inner query - so every replica reads the whole view with no coordination and each
# row comes back once per replica. The `rows through the filter` line is the oracle for that (it
# read `3 * max_parallel_replicas` rows before the shortcut was fenced off); the `no filter` line
# is the control showing the shortcut is engaged for this shape.

db=${CLICKHOUSE_DATABASE}
invoker="user05105_${CLICKHOUSE_DATABASE}_$RANDOM"
definer="definer05105_${CLICKHOUSE_DATABASE}_$RANDOM"

${CLICKHOUSE_CLIENT} <<EOSQL
CREATE TABLE $db.atf_secrets (owner String, secret String) ENGINE = MergeTree ORDER BY owner;
INSERT INTO $db.atf_secrets SELECT 'visible_owner', 'visible_' || toString(number) FROM numbers(3);
INSERT INTO $db.atf_secrets SELECT 'someone_else', 'other_' || toString(number) FROM numbers(3);

CREATE USER $invoker;
CREATE USER $definer;
GRANT SELECT ON $db.atf_secrets TO $definer;
GRANT SELECT ON $db.atf_secrets TO $invoker;
GRANT CREATE TEMPORARY TABLE ON *.* TO $invoker;

-- Projection-only views: they hide no row of the source table by themselves.
CREATE VIEW $db.atf_definer_view DEFINER = $definer SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.atf_secrets;

CREATE VIEW $db.atf_none_view SQL SECURITY NONE
AS SELECT owner, secret FROM $db.atf_secrets;

CREATE VIEW $db.atf_invoker_view SQL SECURITY INVOKER
AS SELECT owner, secret FROM $db.atf_secrets;

GRANT SELECT ON $db.atf_definer_view TO $invoker;
GRANT SELECT ON $db.atf_none_view TO $invoker;
GRANT SELECT ON $db.atf_invoker_view TO $invoker;
EOSQL

# `serialize_query_plan` is pinned by the loop below: with it, the plan (and not the query text) is
# what a replica receives, and the shortcut is chosen for a shape the query text alone would take
# off the parallel-replicas path, which is how the duplication above becomes observable.
PR_SETTINGS="--enable_analyzer 1 --enable_parallel_replicas 1 --max_parallel_replicas 3 \
    --cluster_for_parallel_replicas test_cluster_one_shard_three_replicas_localhost \
    --parallel_replicas_for_non_replicated_merge_tree 1 --parallel_replicas_plan_based 0 \
    --parallel_replicas_allow_view_over_mergetree 1 --parallel_replicas_local_plan 0 \
    --parallel_replicas_min_number_of_rows_per_replica 0 --automatic_parallel_replicas_mode 0"

function parallel_replicas_reads()
{
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} $PR_SETTINGS --serialize_query_plan "$1" --user "$invoker" --query "
        SELECT countIf(explain LIKE '%ReadFromRemoteParallelReplicas%')
        FROM (EXPLAIN indexes = 0 $2)
        FORMAT TSV"
}

function rows_through_the_filter()
{
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} $PR_SETTINGS --serialize_query_plan "$1" --user "$invoker" --query "$2"
}

for serialize in 0 1; do
    for view in atf_definer_view atf_none_view atf_invoker_view; do
        echo "--- $view (serialize_query_plan = $serialize) ---"
        echo -e "no filter, parallel replicas reads:\t$(parallel_replicas_reads "$serialize" "SELECT owner, secret FROM $db.$view")"
        # A barrier view is read locally once an additional table filter is in play; the plan of the
        # `SQL SECURITY INVOKER` twin depends on whether the plan or the query text is shipped, so
        # only the two barrier views are asserted structurally.
        if [[ $view != atf_invoker_view ]]; then
            echo -e "filter on the view, parallel replicas reads:\t$(parallel_replicas_reads "$serialize" "SELECT owner, secret FROM $db.$view
                SETTINGS additional_table_filters = {'$db.$view': 'owner = ''visible_owner'''}")"
            echo -e "filter on the alias, parallel replicas reads:\t$(parallel_replicas_reads "$serialize" "SELECT owner, secret FROM $db.$view AS atf_alias
                SETTINGS additional_table_filters = {'atf_alias': 'owner = ''visible_owner'''}")"
        fi
        echo -e "rows through the filter on the view:\t$(rows_through_the_filter "$serialize" "
            SELECT count() FROM $db.$view
            SETTINGS additional_table_filters = {'$db.$view': 'owner = ''visible_owner'''}")"
        # An alias-keyed `additional_table_filters` entry is not asserted for the
        # `SQL SECURITY INVOKER` twin: it is dropped, and the rows are returned once per replica,
        # for a plain `MergeTree` table read with parallel replicas and a shipped plan as well
        # (`SELECT count() FROM t AS a SETTINGS additional_table_filters = {'a': ...}` returns
        # `count() * max_parallel_replicas` of the unfiltered table), which has nothing to do with
        # a view. The barrier views are asserted because for them the shortcut is fenced off and
        # the read stays local.
        if [[ $view != atf_invoker_view ]]; then
            echo -e "rows through the filter on the alias:\t$(rows_through_the_filter "$serialize" "
                SELECT count() FROM $db.$view AS atf_alias
                SETTINGS additional_table_filters = {'atf_alias': 'owner = ''visible_owner'''}")"
        fi
    done
done

${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.atf_definer_view, $db.atf_none_view, $db.atf_invoker_view"
${CLICKHOUSE_CLIENT} --query "DROP USER $invoker, $definer"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.atf_secrets"
