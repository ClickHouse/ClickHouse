#!/usr/bin/env bash
# Tags: no-ordinary-database

# An unqualified `additional_table_filters` key names a view only when the view is in the current
# database, as in the planner. For a view or a materialized view in another database, such an entry
# must not take part in its `modification_hash`: a non-deterministic filter must not make the hash
# NULL, and a table read only by that filter must not move it. The qualified key does apply.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

OTHER_DATABASE="${CLICKHOUSE_DATABASE}_other"

$CLICKHOUSE_CLIENT -q "
    CREATE DATABASE ${OTHER_DATABASE};
    CREATE TABLE ${OTHER_DATABASE}.src (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE ${OTHER_DATABASE}.target (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE VIEW ${OTHER_DATABASE}.v AS SELECT x FROM ${OTHER_DATABASE}.src;
    CREATE MATERIALIZED VIEW ${OTHER_DATABASE}.mv TO ${OTHER_DATABASE}.target AS SELECT x FROM ${OTHER_DATABASE}.src;
    INSERT INTO ${OTHER_DATABASE}.src VALUES (1), (2);
    CREATE TABLE aux (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO aux VALUES (1);
"

hash_of()
{
    $CLICKHOUSE_CLIENT --additional_table_filters "$2" -q "
        SELECT modification_hash FROM system.tables WHERE database = '${OTHER_DATABASE}' AND name = '$1'"
}

for view in v mv
do
    echo "--- ${view}"
    h=$(hash_of "${view}" "{'${view}': 'rand() >= 0'}")
    [ -n "${h}" ] && [ "${h}" != "\\N" ] && echo "non-deterministic bare key: has a hash" || echo "non-deterministic bare key: no hash (${h})"

    bare_filters="{'${view}': 'x IN (SELECT x FROM ${CLICKHOUSE_DATABASE}.aux)'}"
    qualified_filters="{'${OTHER_DATABASE}.${view}': 'x IN (SELECT x FROM ${CLICKHOUSE_DATABASE}.aux)'}"
    bare_before=$(hash_of "${view}" "${bare_filters}")
    qualified_before=$(hash_of "${view}" "${qualified_filters}")
    $CLICKHOUSE_CLIENT -q "INSERT INTO aux VALUES (2)"
    bare_after=$(hash_of "${view}" "${bare_filters}")
    qualified_after=$(hash_of "${view}" "${qualified_filters}")
    [ "${bare_before}" = "${bare_after}" ] && echo "bare key: a table read by the filter keeps the hash" || echo "bare key: a table read by the filter moves the hash"
    [ "${qualified_before}" != "${qualified_after}" ] && echo "qualified key: a table read by the filter moves the hash" || echo "qualified key: a table read by the filter keeps the hash"
done

$CLICKHOUSE_CLIENT -q "
    DROP TABLE aux SYNC;
    DROP DATABASE ${OTHER_DATABASE} SYNC;
"
