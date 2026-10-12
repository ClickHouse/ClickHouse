#!/usr/bin/env bash
# Tags: no-ordinary-database

# Which `additional_table_filters` entries the query cache and the `modification_hash` of views treat as
# applicable:
# - an unqualified key matches only a table in the current database, as in the planner;
# - the filters in the `SETTINGS` of a subquery inside a filter are inspected too;
# - the filters reachable only through an entry keyed by the view itself take part in its hash.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

OTHER_DATABASE="${CLICKHOUSE_DATABASE}_other"

$CLICKHOUSE_CLIENT -q "
    CREATE DATABASE ${OTHER_DATABASE};
    CREATE TABLE ${OTHER_DATABASE}.t (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO ${OTHER_DATABASE}.t VALUES (1), (2);
    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE helper (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE aux (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE target (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src VALUES (1), (2);
    INSERT INTO helper VALUES (1), (2);
    INSERT INTO aux VALUES (1);
    CREATE VIEW v AS SELECT x FROM src;
    CREATE MATERIALIZED VIEW mv TO target AS SELECT x FROM src;
"

cache_settings="use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_nondeterministic_function_handling = 'throw'"

echo "--- an unqualified key of a table in another database"
# The planner does not apply `{'t': ...}` to `other.t` read from this database, so the non-deterministic
# filter never runs and must not make the query uncachable.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM ${OTHER_DATABASE}.t SETTINGS additional_table_filters = {'t': 'rand() >= 0'}, ${cache_settings}, query_cache_tag = '${CLICKHOUSE_DATABASE}_bare'"
# The qualified key does apply.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM ${OTHER_DATABASE}.t SETTINGS additional_table_filters = {'${OTHER_DATABASE}.t': 'rand() >= 0'}, ${cache_settings}, query_cache_tag = '${CLICKHOUSE_DATABASE}_qualified'" 2>&1 \
    | grep -o -m1 'QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS'

echo "--- a filter inside the SETTINGS of a filter's subquery"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM src SETTINGS additional_table_filters = {'src': 'x IN (SELECT x FROM helper SETTINGS additional_table_filters = {''helper'': ''rand() >= 0''})'}, ${cache_settings}, query_cache_tag = '${CLICKHOUSE_DATABASE}_nested'" 2>&1 \
    | grep -o -m1 'QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS'

hash_of()
{
    $CLICKHOUSE_CLIENT --additional_table_filters "$2" -q "
        SELECT modification_hash FROM system.tables WHERE database = currentDatabase() AND name = '$1'"
}

nested_filters="{'v': 'x IN (SELECT x FROM helper SETTINGS additional_table_filters = {''helper'': ''x IN (SELECT x FROM aux)''})'}"
before=$(hash_of v "${nested_filters}")
$CLICKHOUSE_CLIENT -q "INSERT INTO aux VALUES (2)"
after=$(hash_of v "${nested_filters}")
[ -n "${before}" ] && [ "${before}" != "\\N" ] && echo "view has a hash: yes" || echo "view has a hash: no (${before})"
[ "${before}" != "${after}" ] && echo "a table read by a nested filter moves the hash: yes" || echo "a table read by a nested filter moves the hash: no"

echo "--- filters reachable only through the entry keyed by the view"
for view in v mv
do
    transitive_filters="{'${view}': 'x IN (SELECT x FROM helper)', 'helper': 'x IN (SELECT x FROM aux)'}"
    before=$(hash_of "${view}" "${transitive_filters}")
    $CLICKHOUSE_CLIENT -q "INSERT INTO aux VALUES (3)"
    after=$(hash_of "${view}" "${transitive_filters}")
    [ -n "${before}" ] && [ "${before}" != "\\N" ] && echo "${view} has a hash: yes" || echo "${view} has a hash: no (${before})"
    [ "${before}" != "${after}" ] && echo "${view}: a table read by a transitive filter moves the hash: yes" || echo "${view}: a table read by a transitive filter moves the hash: no"
done

$CLICKHOUSE_CLIENT -q "
    DROP VIEW mv SYNC;
    DROP VIEW v SYNC;
    DROP TABLE target SYNC;
    DROP TABLE aux SYNC;
    DROP TABLE helper SYNC;
    DROP TABLE src SYNC;
    DROP DATABASE ${OTHER_DATABASE} SYNC;
"
