#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `additional_table_filters` entries are keyed by table: an entry filters the table it names and
# nothing else. `StorageView::canHideRows` therefore matches the entries of the view's effective
# security context (a definer profile) and of the view's own `SETTINGS` clause against the source
# table of the view's query - by its qualified name, by its bare name in the current database and
# by its alias - exactly like the interpreters do when they apply the setting. An entry for an
# unrelated table must not turn a projection-only `SQL SECURITY DEFINER` view into an optimization
# barrier (it used to, losing inlining, `PREWHERE` forwarding and the `ORDER BY ... LIMIT` pushdown
# for no security benefit), while an entry that does name the source table hides rows exactly like
# a `WHERE` of the view's query and must fail closed.

db=${CLICKHOUSE_DATABASE}
invoker="user05210_${CLICKHOUSE_DATABASE}_$RANDOM"
plain_definer="plain05210_${CLICKHOUSE_DATABASE}_$RANDOM"
unrelated_definer="unrelated05210_${CLICKHOUSE_DATABASE}_$RANDOM"
bare_unrelated_definer="bareunrelated05210_${CLICKHOUSE_DATABASE}_$RANDOM"
source_definer="source05210_${CLICKHOUSE_DATABASE}_$RANDOM"
bare_source_definer="baresource05210_${CLICKHOUSE_DATABASE}_$RANDOM"
alias_definer="alias05210_${CLICKHOUSE_DATABASE}_$RANDOM"

${CLICKHOUSE_CLIENT} <<EOSQL
CREATE TABLE $db.uatf_secrets (owner String, secret String) ENGINE = MergeTree ORDER BY owner;
INSERT INTO $db.uatf_secrets VALUES ('a_owner', 'visible'), ('z_someone_else', 'other');
CREATE TABLE $db.uatf_unrelated (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO $db.uatf_unrelated VALUES (1), (2);

CREATE USER $invoker;
CREATE USER $plain_definer;
-- Entries for a table the views never read, keyed by the qualified and by the bare name (a profile
-- setting takes the string form of the map).
CREATE USER $unrelated_definer SETTINGS additional_table_filters = '{''$db.uatf_unrelated'': ''id = 1''}';
CREATE USER $bare_unrelated_definer SETTINGS additional_table_filters = '{''uatf_unrelated'': ''id = 1''}';
-- Entries for the source table itself: by qualified name, by bare name and by alias.
CREATE USER $source_definer SETTINGS additional_table_filters = '{''$db.uatf_secrets'': ''length(owner) < 10''}';
CREATE USER $bare_source_definer SETTINGS additional_table_filters = '{''uatf_secrets'': ''length(owner) < 10''}';
CREATE USER $alias_definer SETTINGS additional_table_filters = '{''s'': ''length(owner) < 10''}';

GRANT SELECT ON $db.* TO $plain_definer, $unrelated_definer, $bare_unrelated_definer, $source_definer, $bare_source_definer, $alias_definer, $invoker;

CREATE VIEW $db.uatf_unrelated_view DEFINER = $unrelated_definer SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.uatf_secrets;
CREATE VIEW $db.uatf_bare_unrelated_view DEFINER = $bare_unrelated_definer SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.uatf_secrets;
CREATE VIEW $db.uatf_source_view DEFINER = $source_definer SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.uatf_secrets;
CREATE VIEW $db.uatf_bare_source_view DEFINER = $bare_source_definer SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.uatf_secrets;
CREATE VIEW $db.uatf_invoker_view SQL SECURITY INVOKER
AS SELECT owner, secret FROM $db.uatf_secrets;

CREATE VIEW $db.uatf_alias_view DEFINER = $alias_definer SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.uatf_secrets AS s;
CREATE VIEW $db.uatf_alias_unrelated_view DEFINER = $unrelated_definer SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.uatf_secrets AS s;
CREATE VIEW $db.uatf_alias_invoker_view SQL SECURITY INVOKER
AS SELECT owner, secret FROM $db.uatf_secrets AS s;

-- The same entries written in the view's own SETTINGS clause; the INVOKER twins carry the same clause.
CREATE VIEW $db.uatf_clause_unrelated_view DEFINER = $plain_definer SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.uatf_secrets SETTINGS additional_table_filters = {'$db.uatf_unrelated': 'id = 1'};
CREATE VIEW $db.uatf_clause_unrelated_invoker_view SQL SECURITY INVOKER
AS SELECT owner, secret FROM $db.uatf_secrets SETTINGS additional_table_filters = {'$db.uatf_unrelated': 'id = 1'};
CREATE VIEW $db.uatf_clause_source_view DEFINER = $plain_definer SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.uatf_secrets SETTINGS additional_table_filters = {'$db.uatf_secrets': 'length(owner) < 10'};
CREATE VIEW $db.uatf_clause_source_invoker_view SQL SECURITY INVOKER
AS SELECT owner, secret FROM $db.uatf_secrets SETTINGS additional_table_filters = {'$db.uatf_secrets': 'length(owner) < 10'};
EOSQL

explain_client="${CLICKHOUSE_CLIENT} --user $invoker --enable_parallel_replicas 0
    --query_plan_merge_filters 1 --optimize_move_to_prewhere 0 --query_plan_optimize_prewhere 0"

function plans_the_same()
{
    # shellcheck disable=SC2086
    if diff -q \
        <(${explain_client} $3 --query "EXPLAIN actions = 1, indexes = 0 SELECT * FROM $db.$1 WHERE secret = 'x'" 2>&1) \
        <(${explain_client} $3 --query "EXPLAIN actions = 1, indexes = 0 SELECT * FROM $db.$2 WHERE secret = 'x'" 2>&1) > /dev/null
    then echo "same"; else echo "different"; fi
}

function compare_on_every_analyzer()
{
    for analyzer_settings in "--enable_analyzer 0" "--enable_analyzer 1" "--enable_analyzer 1 --analyzer_inline_views 1"; do
        plans_the_same "$1" "$2" "$analyzer_settings"
    done
}

for view in uatf_unrelated_view uatf_bare_unrelated_view; do
    echo "===== a definer profile entry for an unrelated table keeps the view transparent: $view ====="
    compare_on_every_analyzer "$view" uatf_invoker_view
done
echo "===== the same with an aliased source ====="
compare_on_every_analyzer uatf_alias_unrelated_view uatf_alias_invoker_view
echo "===== an entry for the unrelated table in the view's own SETTINGS clause ====="
compare_on_every_analyzer uatf_clause_unrelated_view uatf_clause_unrelated_invoker_view

for view in uatf_source_view uatf_bare_source_view; do
    echo "===== a definer profile entry for the source table fails closed: $view ====="
    compare_on_every_analyzer "$view" uatf_invoker_view
done
echo "===== an entry keyed by the alias of the source table fails closed ====="
compare_on_every_analyzer uatf_alias_view uatf_alias_invoker_view
echo "===== an entry for the source table in the view's own SETTINGS clause fails closed ====="
compare_on_every_analyzer uatf_clause_source_view uatf_clause_source_invoker_view

echo "===== results: the unrelated entries hide nothing, the source entries do filter ====="
for view in uatf_unrelated_view uatf_bare_unrelated_view uatf_alias_unrelated_view uatf_clause_unrelated_view uatf_source_view uatf_bare_source_view uatf_alias_view uatf_clause_source_view; do
    echo "$view: $(${CLICKHOUSE_CLIENT} --user "$invoker" --query "SELECT groupArray(secret) FROM (SELECT secret FROM $db.$view ORDER BY secret)")"
done

${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.uatf_unrelated_view, $db.uatf_bare_unrelated_view, $db.uatf_source_view, $db.uatf_bare_source_view, $db.uatf_invoker_view, $db.uatf_alias_view, $db.uatf_alias_unrelated_view, $db.uatf_alias_invoker_view, $db.uatf_clause_unrelated_view, $db.uatf_clause_unrelated_invoker_view, $db.uatf_clause_source_view, $db.uatf_clause_source_invoker_view"
${CLICKHOUSE_CLIENT} --query "DROP USER $invoker, $plain_definer, $unrelated_definer, $bare_unrelated_definer, $source_definer, $bare_source_definer, $alias_definer"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.uatf_secrets, $db.uatf_unrelated"
