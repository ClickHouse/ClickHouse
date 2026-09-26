#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the fast test build has no `S3Queue`
#
# What `ALTER ... MODIFY SETTING` does to the reported source, on the engines that are not `MergeTree`.
# `05220` covers `MergeTree`, where a modified setting becomes the definition's. Two more cases matter,
# and they answer differently:
#
#   - a queue engine keeps its settings in Keeper, shared by every table on the same `keeper_path`, so a
#     setting an `ALTER` writes there is still the shared metadata's and not this table's own. Reporting
#     it as `definition` would say the value is local to the table, which is what the source column is
#     for distinguishing;
#   - `Memory` keeps its settings itself, so an `ALTER` does make them the definition's.
#
# A third case cannot be reached and so is not tested: a value a named collection supplied never turns
# into a definition by `ALTER`, because the engines that read collections - `Kafka`, `NATS`, `RabbitMQ`,
# `MySQL`, `PostgreSQL` - are exactly the ones that refuse `MODIFY SETTING` with `NOT_IMPLEMENTED`, and
# the engines that accept it read no collections.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# `keeper_path` is server-wide, so it has to carry this test's database to let two runs coexist.
KEEPER_PATH="/clickhouse/${CLICKHOUSE_DATABASE}/05258"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS q_alter"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS m_alter"

# The endpoint is never reached: the table is created, its settings written to Keeper, and nothing reads
# the bucket.
$CLICKHOUSE_CLIENT -q "
CREATE TABLE q_alter (a UInt64) ENGINE = S3Queue('http://localhost:1/bucketname/data/*', 'key', 'secret', 'TSV')
SETTINGS mode = 'unordered', keeper_path = '${KEEPER_PATH}', loading_retries = 42"

echo "-- a queue engine reports a setting it keeps in Keeper as the shared metadata's"
$CLICKHOUSE_CLIENT -q "
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'q_alter' AND name = 'loading_retries'"

echo "-- and an ALTER writes the new value there, so it stays the shared metadata's"
$CLICKHOUSE_CLIENT -q "ALTER TABLE q_alter MODIFY SETTING loading_retries = 7"
$CLICKHOUSE_CLIENT -q "
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'q_alter' AND name = 'loading_retries'"

echo "-- the statement reads the same table, so it says the same"
$CLICKHOUSE_CLIENT -q "SHOW TABLE SETTINGS FROM q_alter LIKE 'loading_retries'"

$CLICKHOUSE_CLIENT -q "CREATE TABLE m_alter (a UInt64) ENGINE = Memory SETTINGS max_rows_to_keep = 100"

echo "-- a Memory table keeps its own settings, so one it does not state is the compiled-in default"
$CLICKHOUSE_CLIENT -q "
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'm_alter' AND name = 'max_bytes_to_keep'"

echo "-- and an ALTER makes it the definition's, as it does for MergeTree"
$CLICKHOUSE_CLIENT -q "ALTER TABLE m_alter MODIFY SETTING max_bytes_to_keep = 4096"
$CLICKHOUSE_CLIENT -q "
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'm_alter' AND name = 'max_bytes_to_keep'"

echo "-- which is what the stored query says too"
$CLICKHOUSE_CLIENT -q "
SELECT create_table_query LIKE '%max_bytes_to_keep = 4096%' FROM system.tables
WHERE database = currentDatabase() AND name = 'm_alter'"

echo "-- an engine that reads a named collection refuses MODIFY SETTING, so no source can change that way"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS k_alter"
$CLICKHOUSE_CLIENT -q "CREATE TABLE k_alter (a UInt64) ENGINE = Kafka
SETTINGS kafka_broker_list = 'b:9092', kafka_topic_list = 't', kafka_group_name = 'g', kafka_format = 'CSV'"
$CLICKHOUSE_CLIENT -q "ALTER TABLE k_alter MODIFY SETTING kafka_max_block_size = 99" 2>&1 | grep -o -m1 'NOT_IMPLEMENTED'

$CLICKHOUSE_CLIENT -q "DROP TABLE k_alter"
$CLICKHOUSE_CLIENT -q "DROP TABLE m_alter"
$CLICKHOUSE_CLIENT -q "DROP TABLE q_alter"
