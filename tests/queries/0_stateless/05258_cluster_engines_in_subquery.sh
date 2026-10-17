#!/usr/bin/env bash
# Tags: no-old-analyzer
# no-old-analyzer: the replacement of cluster engines by their `*Cluster` variant is decided in the analyzer.

# With `parallel_replicas_allow_in_with_subquery = 0` an `IN` subquery must not be executed on the replicas. The planner
# checks it only once the storages are chosen, and by then a cluster engine (`url`, `s3`, a table of a data lake catalog,
# ...) had already been replaced by its `*Cluster` variant, which ships the query with the subquery to every replica.
# Such a read now stays local instead.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SETTINGS="enable_analyzer = 1, parallel_replicas_for_cluster_engines = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_mode = 'read_tasks',
    automatic_parallel_replicas_mode = 0"
# The read goes to this server: the sum of 0..3 over `n IN (1, 3)` is 4.
URL="url('http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+AS+n+FROM+numbers(4)', TSV, 'n UInt64')"
QUERY="SELECT sum(n) FROM $URL WHERE n IN (SELECT arrayJoin([1, 3]))"

function run()
{
    local query="$1" settings="$2"
    echo "$($CLICKHOUSE_CLIENT -q "EXPLAIN $query SETTINGS $SETTINGS, $settings" 2>&1 | grep -oE -m1 'ReadFrom(URL|Cluster)|SUPPORT_IS_DISABLED')" \
        "$($CLICKHOUSE_CLIENT -q "$query SETTINGS $SETTINGS, $settings" 2>&1 | grep -oE -m1 '^[0-9]+$|SUPPORT_IS_DISABLED')"
}

echo "IN subquery allowed: $(run "$QUERY" "enable_parallel_replicas = 1, parallel_replicas_allow_in_with_subquery = 1")"
echo "IN subquery not allowed: $(run "$QUERY" "enable_parallel_replicas = 1, parallel_replicas_allow_in_with_subquery = 0")"

# `IN` a table or a CTE builds the set from a subquery too.
$CLICKHOUSE_CLIENT -q "CREATE TABLE in_set (x UInt64) ENGINE = MergeTree ORDER BY x; INSERT INTO in_set VALUES (1), (3);" --multiquery
echo "IN table not allowed: $(run "SELECT sum(n) FROM $URL WHERE n IN in_set" "enable_parallel_replicas = 1, parallel_replicas_allow_in_with_subquery = 0")"
echo "IN CTE not allowed: $(run "WITH c AS (SELECT x FROM in_set) SELECT sum(n) FROM $URL WHERE n IN c" "enable_parallel_replicas = 1, parallel_replicas_allow_in_with_subquery = 0")"

# `IN` that appears only while the query is resolved: through an alias of the WITH section, the body of a SQL
# user-defined function, and `EXISTS`, which is rewritten to `IN` unless it is executed as a scalar subquery.
echo "IN in a WITH alias not allowed: $(run "WITH n IN (SELECT arrayJoin([1, 3])) AS cond SELECT sum(n) FROM $URL WHERE cond" "enable_parallel_replicas = 1, parallel_replicas_allow_in_with_subquery = 0")"
echo "IN in a WITH alias used in a subquery not allowed: $(run "WITH n IN (SELECT arrayJoin([1, 3])) AS cond SELECT * FROM (SELECT sum(n) FROM $URL WHERE cond)" "enable_parallel_replicas = 1, parallel_replicas_allow_in_with_subquery = 0")"
UDF="in_udf_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT -q "CREATE FUNCTION $UDF AS (v) -> v IN (SELECT arrayJoin([1, 3]))"
echo "IN in a SQL UDF not allowed: $(run "SELECT sum(n) FROM $URL WHERE $UDF(n)" "enable_parallel_replicas = 1, parallel_replicas_allow_in_with_subquery = 0")"
$CLICKHOUSE_CLIENT -q "DROP FUNCTION $UDF"
echo "EXISTS rewritten to IN not allowed: $(run "SELECT sum(n) FROM $URL WHERE EXISTS (SELECT arrayJoin([1, 3]) AS v WHERE v = 1)" "enable_parallel_replicas = 1, parallel_replicas_allow_in_with_subquery = 0, execute_exists_as_scalar_subquery = 0")"
# In force mode the combination is still rejected.
echo "IN subquery not allowed, force mode: $(run "$QUERY" "enable_parallel_replicas = 2, parallel_replicas_allow_in_with_subquery = 0")"
# `IN` with a set of constants is not a subquery.
echo "IN constants, not allowed: $(run "SELECT sum(n) FROM $URL WHERE n IN (1, 3)" "enable_parallel_replicas = 1, parallel_replicas_allow_in_with_subquery = 0")"
echo "IN an alias of constants, not allowed: $(run "WITH [1, 3] AS s SELECT sum(n) FROM $URL WHERE n IN s" "enable_parallel_replicas = 1, parallel_replicas_allow_in_with_subquery = 0")"
echo "IN a transitive alias of constants, not allowed: $(run "WITH [1, 3] AS s1, s1 AS s2 SELECT sum(n) FROM $URL WHERE n IN s2" "enable_parallel_replicas = 1, parallel_replicas_allow_in_with_subquery = 0")"
echo "IN an outer alias of constants in a subquery, not allowed: $(run "WITH [1, 3] AS s SELECT * FROM (SELECT sum(n) FROM $URL WHERE n IN s)" "enable_parallel_replicas = 1, parallel_replicas_allow_in_with_subquery = 0")"
echo "IN an alias of a scalar subquery, not allowed: $(run "WITH (SELECT [1, 3]) AS s SELECT sum(n) FROM $URL WHERE n IN s" "enable_parallel_replicas = 1, parallel_replicas_allow_in_with_subquery = 0")"
# A SQL user-defined function without a subquery in its body.
UDF="plain_udf_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT -q "CREATE FUNCTION $UDF AS (v) -> v IN (1, 3)"
echo "SQL UDF without a subquery, not allowed: $(run "SELECT sum(n) FROM $URL WHERE $UDF(n)" "enable_parallel_replicas = 1, parallel_replicas_allow_in_with_subquery = 0")"
$CLICKHOUSE_CLIENT -q "DROP FUNCTION $UDF"
# A scalar subquery in a SQL user-defined function is executed before the query is sent to the replicas.
UDF="scalar_udf_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT -q "CREATE FUNCTION $UDF AS () -> (SELECT 1)"
echo "SQL UDF with a scalar subquery, not allowed: $(run "SELECT sum(n) FROM $URL WHERE $UDF() = 1" "enable_parallel_replicas = 1, parallel_replicas_allow_in_with_subquery = 0")"
$CLICKHOUSE_CLIENT -q "DROP FUNCTION $UDF"
# A SQL user-defined function with `IN` a parameter: the argument is resolved before it is bound, so it is never a set
# built from a subquery.
UDF="param_udf_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT -q "CREATE FUNCTION $UDF AS (s, v) -> v IN s"
echo "SQL UDF with IN a parameter, not allowed: $(run "SELECT sum(n) FROM $URL WHERE $UDF([1, 3], n)" "enable_parallel_replicas = 1, parallel_replicas_allow_in_with_subquery = 0")"
# A SQL user-defined function with `IN` a table.
$CLICKHOUSE_CLIENT -q "CREATE OR REPLACE FUNCTION $UDF AS (v) -> v IN in_set"
echo "SQL UDF with IN a table, not allowed: $(run "SELECT sum(n) FROM $URL WHERE $UDF(n)" "enable_parallel_replicas = 1, parallel_replicas_allow_in_with_subquery = 0")"
$CLICKHOUSE_CLIENT -q "DROP FUNCTION $UDF"
