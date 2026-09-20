#!/usr/bin/env bash
# Tags: no-fasttest
# Tag justification: needs the Kafka engine, which is an optional build.
#
# What a named collection holds is secret as a whole: `system.named_collections` hides every key of it without
# `SHOW NAMED COLLECTIONS SECRETS`, and `SHOW CREATE TABLE` prints the collection's name rather than what it
# holds. `system.table_settings` needs only `SHOW TABLES` on the table, so it must not be the surface that
# hands a collection's contents to a user the other two refuse - a broker address says as much about where a
# table points as a password does.
#
# A shell test because a named collection is server-wide, so its name has to carry this test's database, and
# because the reader has to be a user of its own.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

NC="nc_${CLICKHOUSE_DATABASE}"
READER="reader_${CLICKHOUSE_DATABASE}"

# Re-runnable: the flaky check runs a test many times against the same database.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS kafka_from_collection"
$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION IF EXISTS ${NC}"
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${READER}"

$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION ${NC} AS
    kafka_broker_list = 'secret-broker.invalid:9092', kafka_topic_list = 'secret_topic',
    kafka_group_name = 'secret_group', kafka_format = 'CSV'"
$CLICKHOUSE_CLIENT -q "CREATE TABLE kafka_from_collection (a UInt64) ENGINE = Kafka(${NC})"

$CLICKHOUSE_CLIENT -q "CREATE USER ${READER} IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.* TO ${READER}"
$CLICKHOUSE_CLIENT -q "GRANT SHOW TABLES ON ${CLICKHOUSE_DATABASE}.kafka_from_collection TO ${READER}"

echo "-- the reader may read every system table, and still sees no collection"
$CLICKHOUSE_CLIENT --user "${READER}" -q "SELECT count() FROM system.named_collections"

echo "-- nor may it read the table's definition, which would name the collection"
$CLICKHOUSE_CLIENT --user "${READER}" -q "SHOW CREATE TABLE ${CLICKHOUSE_DATABASE}.kafka_from_collection" 2>&1 \
    | grep -o -m1 'ACCESS_DENIED'

echo "-- so system.table_settings names the settings the collection supplied, and hides their values"
$CLICKHOUSE_CLIENT --user "${READER}" -q "
    SELECT name, value, is_masked, source FROM system.table_settings
    WHERE database = '${CLICKHOUSE_DATABASE}' AND table = 'kafka_from_collection' AND source = 'named_collection'
    ORDER BY name"

echo "-- and a setting the table's own clause states is reported as before"
$CLICKHOUSE_CLIENT -q "DROP TABLE kafka_from_collection"
$CLICKHOUSE_CLIENT -q "CREATE TABLE kafka_from_collection (a UInt64) ENGINE = Kafka(${NC}) SETTINGS kafka_max_block_size = 4242"
$CLICKHOUSE_CLIENT --user "${READER}" -q "
    SELECT name, value, is_masked, source FROM system.table_settings
    WHERE database = '${CLICKHOUSE_DATABASE}' AND table = 'kafka_from_collection' AND name = 'kafka_max_block_size'"

$CLICKHOUSE_CLIENT -q "DROP TABLE kafka_from_collection"
$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION ${NC}"
$CLICKHOUSE_CLIENT -q "DROP USER ${READER}"
