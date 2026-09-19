#!/usr/bin/env bash
# Tags: no-fasttest
# Tag justification: needs the Kafka engine, which is an optional build.
#
# A named collection is a source of its own in `system.table_settings`: the settings object records
# which of its values the collection supplied, so the table can say so for exactly those settings.
#
# A shell test rather than a `.sql` one because named collections are server-wide: the name has to
# carry this test's database so two parallel runs do not collide on it, and `CREATE NAMED COLLECTION`
# does not accept a query parameter in the name position.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

NC="nc_${CLICKHOUSE_DATABASE}"

# Re-runnable: the flaky check runs a test many times against the same database.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS knc_tbl"
$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION IF EXISTS ${NC}"
$CLICKHOUSE_CLIENT -q "
CREATE NAMED COLLECTION ${NC} AS
    kafka_broker_list = 'b:9092', kafka_topic_list = 't', kafka_group_name = 'g',
    kafka_format = 'CSV', kafka_max_block_size = 4242"

$CLICKHOUSE_CLIENT -q "CREATE TABLE knc_tbl (a UInt64) ENGINE = Kafka(${NC})"

echo "-- what the collection supplied, and the three rows it did not"
# The `other` rows are the finding rather than noise: `StorageKafka`'s constructor pins those format
# settings itself, so they come from neither the collection nor the table's definition. The client id it
# generates is `other` too, but carries the host name, so it is left out here; `05213` covers it.
$CLICKHOUSE_CLIENT -q "
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'knc_tbl' AND source != 'default' AND name != 'kafka_client_id'
ORDER BY name"

echo "-- a table's own SETTINGS clause wins over the collection"
$CLICKHOUSE_CLIENT -q "DROP TABLE knc_tbl"
$CLICKHOUSE_CLIENT -q "CREATE TABLE knc_tbl (a UInt64) ENGINE = Kafka(${NC}) SETTINGS kafka_max_block_size = 17"
$CLICKHOUSE_CLIENT -q "
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'knc_tbl' AND name IN ('kafka_max_block_size', 'kafka_topic_list')
ORDER BY name"

$CLICKHOUSE_CLIENT -q "DROP TABLE knc_tbl"
$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION ${NC}"
