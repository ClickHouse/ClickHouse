#!/usr/bin/env bash
# Tags:no-parallel,shard

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
# before SELECT * FROM local
query_countI=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.events WHERE event = 'InitialQuery'")
query_countQ=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.events WHERE event = 'Query'")

# Execute SELECT * FROM local
$CLICKHOUSE_CLIENT -q "SELECT * FROM local" > /dev/null

# Counts after SELECT * FROM local
After_query_countI=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.events WHERE event = 'InitialQuery'")
After_query_countQ=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.events WHERE event = 'Query'")

# Calculate the differences
Initial_query_diff=$(($After_query_countI-$query_countI-2))
query_diff=$(($After_query_countQ-$query_countQ-2))

echo "Initial Query Difference: $Initial_query_diff"
echo "Query Difference: $query_diff"
echo "Distributed situation"

# before SELECT * FROM distributed
query_countI=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.events WHERE event = 'InitialQuery'")
query_countQ=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.events WHERE event = 'Query'")

# Execute SELECT * FROM distributed
$CLICKHOUSE_CLIENT -q "SELECT * FROM distributed SETTINGS prefer_localhost_replica = 0" > /dev/null

# Counts after SELECT * FROM distributed
After_query_countI=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.events WHERE event = 'InitialQuery'")
After_query_countQ=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.events WHERE event = 'Query'")

# Calculate the differences
Initial_query_diff=$(($After_query_countI-$query_countI-2))
query_diff=$(($After_query_countQ-$query_countQ-2))

echo "Initial Query Difference: $Initial_query_diff"
echo "Query Difference: $query_diff"
