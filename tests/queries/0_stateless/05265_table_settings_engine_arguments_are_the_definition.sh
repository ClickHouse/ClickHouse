#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs the Kafka engine, which is an optional build.
#
# A value a table's engine arguments state is the table's definition, as surely as its `SETTINGS` clause: the
# `index_granularity` of the old `MergeTree(date, key, granularity)` syntax, and a key-value argument overriding a
# named collection's key, `Kafka(collection, key = value)`. Both used to report `other`, a value the engine chose.
# The override is contrasted with a key the collection supplies itself, which stays the collection's.
#
# A shell test because a named collection is server-wide, so its name has to carry this test's database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

NC="nc_${CLICKHOUSE_DATABASE}"

# Re-runnable: the flaky check runs a test many times against the same database.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS old_syntax"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS kafka_override"
$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION IF EXISTS ${NC}"

echo "-- the granularity of the old MergeTree syntax, before and after the table is loaded again"
$CLICKHOUSE_CLIENT --allow_deprecated_syntax_for_merge_tree 1 -q "
CREATE TABLE old_syntax (d Date, x UInt64) ENGINE = MergeTree(d, x, 4096)"
$CLICKHOUSE_CLIENT -q "
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'old_syntax' AND name = 'index_granularity'"
$CLICKHOUSE_CLIENT -q "DETACH TABLE old_syntax"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE old_syntax"
$CLICKHOUSE_CLIENT -q "
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'old_syntax' AND name = 'index_granularity'"

echo "-- a key the engine arguments override, next to one the collection supplies"
$CLICKHOUSE_CLIENT -q "
CREATE NAMED COLLECTION ${NC} AS
    kafka_broker_list = 'broker.invalid:9092', kafka_topic_list = 'topic', kafka_group_name = 'group',
    kafka_format = 'CSV', kafka_max_block_size = 100, kafka_skip_broken_messages = 3"
$CLICKHOUSE_CLIENT -q "CREATE TABLE kafka_override (a UInt64) ENGINE = Kafka(${NC}, kafka_max_block_size = 5)"
$CLICKHOUSE_CLIENT -q "
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'kafka_override'
  AND name IN ('kafka_max_block_size', 'kafka_skip_broken_messages')
ORDER BY name"

$CLICKHOUSE_CLIENT -q "DROP TABLE old_syntax"
$CLICKHOUSE_CLIENT -q "DROP TABLE kafka_override"
$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION ${NC}"
