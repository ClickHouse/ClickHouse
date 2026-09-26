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
# In force mode the combination is still rejected.
echo "IN subquery not allowed, force mode: $(run "$QUERY" "enable_parallel_replicas = 2, parallel_replicas_allow_in_with_subquery = 0")"
# `IN` with a set of constants is not a subquery.
echo "IN constants, not allowed: $(run "SELECT sum(n) FROM $URL WHERE n IN (1, 3)" "enable_parallel_replicas = 1, parallel_replicas_allow_in_with_subquery = 0")"
