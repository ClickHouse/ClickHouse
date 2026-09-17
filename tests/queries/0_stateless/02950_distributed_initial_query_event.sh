#!/usr/bin/env bash
# Tags: shard

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# CREATE TABLE local (x UInt8) Engine=Memory;
# CREATE TABLE distributed ON CLUSTER cluster (p Date, i Int32) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), x)
$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS local;
DROP TABLE IF EXISTS distributed;
CREATE TABLE local (x UInt8) Engine=Memory;
CREATE TABLE distributed AS local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), local, x);
INSERT INTO distributed SELECT number FROM numbers(10);
SYSTEM FLUSH DISTRIBUTED distributed;
"
echo "Local situation"

# Count this query's own lineage in query_log (itself plus any dispatched shard sub-queries)
# via initial_query_id, instead of the process-wide 'InitialQuery'/'Query' system.events
# counters, which any concurrent query anywhere on the server would perturb.
query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_local"
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "SELECT * FROM local" > /dev/null
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

Initial_query_diff=$($CLICKHOUSE_CLIENT -q "SELECT countIf(is_initial_query) FROM system.query_log WHERE initial_query_id = '$query_id' AND type != 'QueryStart' AND current_database IN (currentDatabase(), 'default')")
query_diff=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.query_log WHERE initial_query_id = '$query_id' AND type != 'QueryStart' AND current_database IN (currentDatabase(), 'default')")

echo "Initial Query Difference: $Initial_query_diff"
echo "Query Difference: $query_diff"
echo "Distributed situation"

query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_distributed"
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "SELECT * FROM distributed SETTINGS prefer_localhost_replica = 0" > /dev/null
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

Initial_query_diff=$($CLICKHOUSE_CLIENT -q "SELECT countIf(is_initial_query) FROM system.query_log WHERE initial_query_id = '$query_id' AND type != 'QueryStart' AND current_database IN (currentDatabase(), 'default')")
query_diff=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.query_log WHERE initial_query_id = '$query_id' AND type != 'QueryStart' AND current_database IN (currentDatabase(), 'default')")

echo "Initial Query Difference: $Initial_query_diff"
echo "Query Difference: $query_diff"
