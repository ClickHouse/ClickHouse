#!/usr/bin/env bash

# `additional_table_filters` apply to the rows read through a `*Cluster` table function the same way as through a
# plain table function: an entry keyed by the alias applies, it applies before the query's own `WHERE`, and the entries
# for the other tables of the query keep applying.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Every replica of the cluster is this server, and the URL returns the numbers 0..3.
URL="http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+AS+n+FROM+numbers(4)"
SOURCE="urlCluster('test_cluster_one_shard_three_replicas_localhost', '$URL', TSV, 'n UInt64')"

# Keyed by the alias.
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'t': 'n > 1'}"
$CLICKHOUSE_CLIENT -q "SELECT n FROM $SOURCE AS t ORDER BY n SETTINGS additional_table_filters = {'t': 'n > 1'}"
$CLICKHOUSE_CLIENT -q "SELECT sum(t.n) FROM $SOURCE AS t INNER JOIN numbers(4) AS r ON t.n = r.number SETTINGS additional_table_filters = {'t': 'n > 1'}"
$CLICKHOUSE_CLIENT -q "SELECT sum(t.n) FROM numbers(4) AS r INNER JOIN $SOURCE AS t ON r.number = t.n SETTINGS additional_table_filters = {'t': 'n > 1'}"

# Keyed by the full name of the table function.
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'_table_function.urlCluster': 'n > 1'}"

# The first entry naming the table applies, as for a plain table function.
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'t': 'n > 1', '_table_function.urlCluster': 'n > 2'}"

# The query's own WHERE does not see the rows the filter removes.
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t WHERE throwIf(n = 0) = 0 SETTINGS additional_table_filters = {'t': 'n > 1'}, short_circuit_function_evaluation = 'disable', query_plan_merge_filters = 0"

# The entry for another table of the query keeps applying.
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t WHERE n IN (SELECT number FROM numbers(4)) SETTINGS additional_table_filters = {'t': 'n > 1', '_table_function.numbers': 'number < 3'}"

# The tables in a filter are those of the current database.
$CLICKHOUSE_CLIENT -q "CREATE TABLE ids (id UInt64) ENGINE = Memory AS SELECT arrayJoin([2, 3])"
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'t': 'n IN (SELECT id FROM ids)'}"

# The aliases defined in a filter are not table names, with either key.
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'t': 'n IN ([2, 3] AS allowed) AND n IN allowed'}"
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'_table_function.urlCluster': 'n IN ([2, 3] AS allowed) AND n IN allowed'}"

# A filter, and the body of a SQL function in it, see the current database of the query.
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'t': 'n > 1 AND currentDatabase() = \'$CLICKHOUSE_DATABASE\' AND current_schema() = \'$CLICKHOUSE_DATABASE\''}"
$CLICKHOUSE_CLIENT -q "CREATE FUNCTION ${CLICKHOUSE_DATABASE}_in_ids AS (x) -> x IN (SELECT id FROM ids)"
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'t': '${CLICKHOUSE_DATABASE}_in_ids(n)'}"
$CLICKHOUSE_CLIENT -q "DROP FUNCTION ${CLICKHOUSE_DATABASE}_in_ids"
