#!/usr/bin/env bash
# Tags: no-fasttest
# Tag justification: needs the Kafka engine, which is an optional build.
#
# A value a named collection supplied is hidden, as `system.named_collections` hides it, so these rows read
# `[HIDDEN]`; a setting the engine pinned is the engine's own value and is shown.
#
# A setting a named collection supplied is reported as `named_collection` - unless something replaced it
# afterwards. With `kafka_handle_error_mode = 'stream'` the engine pins `input_format_allow_errors_num` to 0
# whatever it was given, so a collection's value for it is not what the table works with, and the row has to
# say the engine set it rather than name a collection that holds a different value. The same holds for the
# other pinned setting, `input_format_allow_errors_ratio`.
#
# A shell test because named collections are server-wide: the name carries this test's database, so that
# parallel runs do not collide, and `CREATE NAMED COLLECTION` takes no query parameter in the name position.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

NC="nc_${CLICKHOUSE_DATABASE}"

# Re-runnable: the flaky check runs a test many times against the same database.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS kafka_pinned"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS kafka_not_pinned"
$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION IF EXISTS ${NC}"
$CLICKHOUSE_CLIENT -q "
CREATE NAMED COLLECTION ${NC} AS
    kafka_broker_list = 'b:9092', kafka_topic_list = 't', kafka_group_name = 'g',
    kafka_format = 'JSONEachRow', input_format_allow_errors_num = 5, input_format_allow_errors_ratio = 0.5,
    kafka_max_block_size = 4242"

$CLICKHOUSE_CLIENT -q "CREATE TABLE kafka_pinned (a UInt64) ENGINE = Kafka(${NC}) SETTINGS kafka_handle_error_mode = 'stream'"
$CLICKHOUSE_CLIENT -q "CREATE TABLE kafka_not_pinned (a UInt64) ENGINE = Kafka(${NC})"

echo "-- pinned by the engine: its own value, not the collection's; the rest is still the collection's"
$CLICKHOUSE_CLIENT -q "
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'kafka_pinned'
    AND name IN ('input_format_allow_errors_num', 'input_format_allow_errors_ratio', 'kafka_max_block_size')
ORDER BY name"

echo "-- not pinned: all are the collection's"
$CLICKHOUSE_CLIENT -q "
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'kafka_not_pinned'
    AND name IN ('input_format_allow_errors_num', 'input_format_allow_errors_ratio', 'kafka_max_block_size')
ORDER BY name"

$CLICKHOUSE_CLIENT -q "DROP TABLE kafka_pinned"
$CLICKHOUSE_CLIENT -q "DROP TABLE kafka_not_pinned"
$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION ${NC}"
