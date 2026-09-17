#!/usr/bin/env bash
# Tags: no-fasttest, zookeeper, no-replicated-database
# Tag no-fasttest: needs the Kafka engine, which is an optional build.
# Tag zookeeper: a `Kafka` table that keeps its offsets in Keeper connects to it when it is created.
# Tag no-replicated-database: the tables state an explicit Keeper path and replica name, which every replica of a
# Replicated database would claim at once.
#
# The `Kafka` engine is backed by two storages: `StorageKafka`, and `StorageKafka2` for a table that keeps its
# offsets in Keeper (`kafka_keeper_path` and `kafka_replica_name`). Both hold the same settings and derive the same
# working values from them, so `system.table_settings` reports them the same way. `StorageKafka2` used to fall back
# to the base implementation and report only its `SETTINGS` clause, with its macros unexpanded.
#
# A shell test because the named collection is server-wide, so its name has to carry this test's database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

NC="nc_${CLICKHOUSE_TEST_UNIQUE_NAME}"
CLAUSE="kafka_broker_list = 'localhost:9092', kafka_topic_list = '{database}_in', kafka_group_name = '{database}_group', kafka_format = 'JSONEachRow'"
SAME_ROWS="name NOT IN ('kafka_keeper_path', 'kafka_replica_name', 'kafka_client_id')"

# Re-runnable: the flaky check runs a new test many times against the same database.
$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS k_plain SYNC;
DROP TABLE IF EXISTS k_keeper SYNC;
DROP TABLE IF EXISTS k_keeper_nc SYNC"
$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION IF EXISTS ${NC}"
$CLICKHOUSE_CLIENT -q "
CREATE NAMED COLLECTION ${NC} AS
    kafka_broker_list = 'b:9092', kafka_topic_list = 't', kafka_group_name = 'g', kafka_format = 'CSV', kafka_max_block_size = 4242"

$CLICKHOUSE_CLIENT -q "CREATE TABLE k_plain (a String) ENGINE = Kafka SETTINGS ${CLAUSE}"
$CLICKHOUSE_CLIENT --allow_kafka_offsets_storage_in_keeper 1 -q "
CREATE TABLE k_keeper (a String) ENGINE = Kafka
SETTINGS ${CLAUSE}, kafka_keeper_path = '/clickhouse/{database}/k_keeper', kafka_replica_name = 'r1'"

echo "-- a Keeper-backed table reports every setting, as a plain one does"
$CLICKHOUSE_CLIENT -q "
SELECT table, count() = (SELECT count() FROM system.engine_settings WHERE engine_name = 'Kafka') AS all_reported
FROM system.table_settings
WHERE database = currentDatabase() AND table IN ('k_plain', 'k_keeper')
GROUP BY table ORDER BY table"

echo "-- and, apart from the two Keeper settings and the client id, the same rows in both directions"
# The client id carries the table name when the engine generates it, so it is checked on its own below.
$CLICKHOUSE_CLIENT -q "
SELECT count() FROM (
    SELECT name, value, source FROM system.table_settings
    WHERE database = currentDatabase() AND table = 'k_keeper' AND ${SAME_ROWS}
    EXCEPT
    SELECT name, value, source FROM system.table_settings
    WHERE database = currentDatabase() AND table = 'k_plain' AND ${SAME_ROWS})"
$CLICKHOUSE_CLIENT -q "
SELECT count() FROM (
    SELECT name, value, source FROM system.table_settings
    WHERE database = currentDatabase() AND table = 'k_plain' AND ${SAME_ROWS}
    EXCEPT
    SELECT name, value, source FROM system.table_settings
    WHERE database = currentDatabase() AND table = 'k_keeper' AND ${SAME_ROWS})"

echo "-- the working values are reported with their macros expanded"
$CLICKHOUSE_CLIENT -q "
SELECT name, value = currentDatabase() || '_in' AS expanded, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'k_keeper' AND name = 'kafka_topic_list'"

echo "-- the Keeper settings come from the definition, the path with its macros expanded"
$CLICKHOUSE_CLIENT -q "
SELECT name, replaceOne(value, currentDatabase(), 'DATABASE'), source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'k_keeper' AND name IN ('kafka_keeper_path', 'kafka_replica_name')
ORDER BY name"

echo "-- the client id the engine generates is reported as the engine's"
$CLICKHOUSE_CLIENT -q "
SELECT name, endsWith(value, '-' || currentDatabase() || '-k_keeper') AS generated, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'k_keeper' AND name = 'kafka_client_id'"

echo "-- a named collection is reported as the source of what it supplies, and the definition still wins"
$CLICKHOUSE_CLIENT --allow_kafka_offsets_storage_in_keeper 1 -q "
CREATE TABLE k_keeper_nc (a String) ENGINE = Kafka(${NC})
SETTINGS kafka_max_block_size = 17, kafka_keeper_path = '/clickhouse/{database}/k_keeper_nc', kafka_replica_name = 'r1'"
$CLICKHOUSE_CLIENT -q "
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'k_keeper_nc' AND name IN ('kafka_max_block_size', 'kafka_topic_list')
ORDER BY name"

echo "-- no row claims the default source for a value that is not the default"
$CLICKHOUSE_CLIENT -q "
SELECT count() FROM system.table_settings
WHERE database = currentDatabase() AND table IN ('k_plain', 'k_keeper', 'k_keeper_nc')
  AND source = 'default' AND value != \`default\`"

$CLICKHOUSE_CLIENT -q "
DROP TABLE k_keeper_nc SYNC;
DROP TABLE k_keeper SYNC;
DROP TABLE k_plain SYNC"
$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION ${NC}"
