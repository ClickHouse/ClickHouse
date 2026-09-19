#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `StorageView::canHideRows` treats an `additional_table_filters` entry keyed by the source table of
# a `SQL SECURITY DEFINER` / `NONE` view - from the definer's profile or from the `SETTINGS` clause of
# the view's own query - as a `WHERE` of the view, which makes the view a barrier. The proof must
# match exactly the names the filter-application paths match: the table expression of the query (its
# alias, its bare or qualified name, the storage id it resolves to). When the source is an `Alias`
# table, an entry keyed by the *target* of the `Alias` is never applied - `StorageAlias::read`
# forwards the already parsed filter and matches nothing again - so it must not make a
# projection-only view a barrier either: the view keeps the optimizations of its `INVOKER` twin.
# An entry keyed by the `Alias` itself is applied, so it does make the view a barrier.

user="user05217_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} --allow_experimental_alias_table_engine 1 <<EOSQL
-- \`key\` is the sort key, so it is what the outer predicate would prune on.
CREATE TABLE $db.owned (key UInt64, owner String)
ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1024;
INSERT INTO $db.owned SELECT number, 'nobody' FROM numbers(100000);

CREATE TABLE $db.owned_alias ENGINE = Alias('$db', 'owned');

-- Projection-only views over the Alias. The entry of the first pair names the target of the Alias,
-- which the read of the Alias never matches; the entry of the second pair names the Alias itself.
CREATE VIEW $db.target_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT * FROM $db.owned_alias SETTINGS additional_table_filters = {'$db.owned': 'key < 10'};
CREATE VIEW $db.target_none SQL SECURITY NONE
AS SELECT * FROM $db.owned_alias SETTINGS additional_table_filters = {'$db.owned': 'key < 10'};
CREATE VIEW $db.target_invoker SQL SECURITY INVOKER
AS SELECT * FROM $db.owned_alias SETTINGS additional_table_filters = {'$db.owned': 'key < 10'};

CREATE VIEW $db.alias_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT * FROM $db.owned_alias SETTINGS additional_table_filters = {'$db.owned_alias': 'key < 10'};
CREATE VIEW $db.alias_none SQL SECURITY NONE
AS SELECT * FROM $db.owned_alias SETTINGS additional_table_filters = {'$db.owned_alias': 'key < 10'};
CREATE VIEW $db.alias_invoker SQL SECURITY INVOKER
AS SELECT * FROM $db.owned_alias SETTINGS additional_table_filters = {'$db.owned_alias': 'key < 10'};

DROP USER IF EXISTS $user;
CREATE USER $user;
GRANT SELECT ON $db.* TO $user;
EOSQL

# The semantics the proof has to mirror: an entry keyed by the target of the Alias filters nothing
# when the query reads the Alias, an entry keyed by the Alias filters the read. Neither depends on
# the security type of the view.
echo "===== rows through the views ====="
for view in target_definer target_none target_invoker alias_definer alias_none alias_invoker; do
    rows=$(${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT count() FROM $db.$view")
    echo -e "$view:\t$rows"
done

echo "===== the plan of a barrier-capable view against the plan of its invoker twin ====="
# The settings pin the plan shape, because the test also runs with randomized settings. An entry keyed
# by the target leaves the view transparent, so the plans agree; an entry keyed by the Alias hides rows,
# so the definer view is sealed and the plans differ.
explain_of() {
    ${CLICKHOUSE_CLIENT} --user "$user" --enable_parallel_replicas 0 \
        --query_plan_merge_filters 1 --optimize_move_to_prewhere 0 --query_plan_optimize_prewhere 0 \
        --query "EXPLAIN actions = 0, description = 0 SELECT count() FROM $db.$1 WHERE key = 99999"
}
for keyed in target alias; do
    for view in definer none; do
        if diff <(explain_of "${keyed}_${view}") <(explain_of "${keyed}_invoker") > /dev/null
        then verdict="same"; else verdict="differs"; fi
        echo -e "${keyed}_${view}:\t$verdict"
    done
done

${CLICKHOUSE_CLIENT} --query "DROP USER $user"
${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.target_definer, $db.target_none, $db.target_invoker, $db.alias_definer, $db.alias_none, $db.alias_invoker"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.owned_alias, $db.owned"
