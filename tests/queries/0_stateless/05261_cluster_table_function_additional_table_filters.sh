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
URL_ARR="http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+AS+n%2C+%5B2%2C3%5D+AS+arr+FROM+numbers(4)"
SOURCE_ARR="urlCluster('test_cluster_one_shard_three_replicas_localhost', '$URL_ARR', TSV, 'n UInt64, arr Array(UInt64)')"
URL_SUB="http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+AS+n%2C+tuple(%5B2%2C3%5D)+AS+tup%2C+%5B2%2C3%5D+AS+x+FROM+numbers(4)"
SOURCE_SUB="urlCluster('test_cluster_one_shard_three_replicas_localhost', '$URL_SUB', TSV, 'n UInt64, tup Tuple(arr Array(UInt64)), nest Nested(x UInt64)')"
URL_DB="http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+AS+n%2C+%27x%27+AS+database+FROM+numbers(4)"
SOURCE_DB="urlCluster('test_cluster_one_shard_three_replicas_localhost', '$URL_DB', TSV, 'n UInt64, database String')"

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

# The aliases, the columns and the lambda parameters in a filter are not table names, with either key.
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'t': 'n IN ([2, 3] AS allowed) AND n IN allowed'}"
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'_table_function.urlCluster': 'n IN ([2, 3] AS allowed) AND n IN allowed'}"
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE_ARR AS t SETTINGS additional_table_filters = {'t': 'n IN arr'}"
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE_ARR AS t SETTINGS additional_table_filters = {'_table_function.urlCluster': 'n IN arr'}"
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE_SUB AS t SETTINGS additional_table_filters = {'t': 'n IN \`tup.arr\` AND tuple(n) IN nest'}"
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE_SUB AS t SETTINGS additional_table_filters = {'_table_function.urlCluster': 'n IN \`tup.arr\` AND tuple(n) IN nest'}"
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'t': 'n > 1 AND \'x\' NOT IN _tags'}"
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'_table_function.urlCluster': 'n > 1 AND \'x\' NOT IN _tags'}"
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'t': 'arrayExists(x -> n IN x, [[2, 3]])'}"
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'_table_function.urlCluster': 'arrayExists(x -> n IN x, [[2, 3]])'}"
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'t': 'n IN ids AND arrayExists(ids -> ids = 1, [1])'}"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ids_allowed (id UInt64, allowed Array(UInt64)) ENGINE = Memory AS SELECT arrayJoin([2, 3]), [2, 3]"
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'t': 'n IN (SELECT id FROM ids_allowed WHERE id IN allowed)'}"
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'_table_function.urlCluster': 'n IN (SELECT id FROM ids_allowed WHERE id IN allowed)'}"

# A filter sees the current database of the query.
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'t': 'n > 1 AND currentDatabase() = \'$CLICKHOUSE_DATABASE\' AND current_schema() = \'$CLICKHOUSE_DATABASE\''}"
NILADIC="CURRENT_DATABASE = \'$CLICKHOUSE_DATABASE\' AND DATABASE = \'$CLICKHOUSE_DATABASE\' AND SCHEMA = \'$CLICKHOUSE_DATABASE\' AND CURRENT_SCHEMA = \'$CLICKHOUSE_DATABASE\'"
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'t': 'n > 1 AND $NILADIC'}"
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'_table_function.urlCluster': 'n > 1 AND $NILADIC'}"
# Unless the name is a column, an alias or a lambda parameter, in its scope.
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE_DB AS t SETTINGS additional_table_filters = {'t': 'n > 1 AND database = \'x\' AND (\'y\' AS schema) = schema AND arrayExists(current_schema -> current_schema = \'z\', [\'z\'])'}"
LAMBDA_ALIAS="arrayExists(x -> (x AS current_database) = current_database, [\'a\']) AND current_database = \'$CLICKHOUSE_DATABASE\'"
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'t': 'n > 1 AND $LAMBDA_ALIAS'}"
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'_table_function.urlCluster': 'n > 1 AND $LAMBDA_ALIAS'}"
# A call of a lambda with the alias of the same name stays a call; `x AS x` and the alias of a call are not such a lambda.
SHADOW="(WITH (() -> \'shadow\') AS currentDatabase SELECT currentDatabase()) = \'shadow\'"
NOT_SHADOW="(current_database AS current_database) = \'$CLICKHOUSE_DATABASE\' AND (currentDatabase() AS currentDatabase) = \'$CLICKHOUSE_DATABASE\'"
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'t': 'n > 1 AND $SHADOW'}"
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'_table_function.urlCluster': 'n > 1 AND $SHADOW'}"
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'t': 'n > 1 AND $NOT_SHADOW'}"
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'_table_function.urlCluster': 'n > 1 AND $NOT_SHADOW'}"

# The argument of a SQL function in a filter is not captured by a lambda parameter of the same name, with either key.
$CLICKHOUSE_CLIENT -q "CREATE FUNCTION ${CLICKHOUSE_DATABASE}_any_greater AS threshold -> arrayExists(n -> n > threshold, [3])"
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'t': '${CLICKHOUSE_DATABASE}_any_greater(n)'}"
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'_table_function.urlCluster': '${CLICKHOUSE_DATABASE}_any_greater(n)'}"
$CLICKHOUSE_CLIENT -q "DROP FUNCTION ${CLICKHOUSE_DATABASE}_any_greater"
