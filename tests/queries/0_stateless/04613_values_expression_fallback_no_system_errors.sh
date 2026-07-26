#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: `system.errors` keeps only the latest query ID for each error code.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

fallback_query_id="04613_values_fallback_${CLICKHOUSE_DATABASE}_${RANDOM}"
overflow_query_id="04613_values_overflow_${CLICKHOUSE_DATABASE}_${RANDOM}"
overflow_retry_query_id="04613_values_overflow_retry_${CLICKHOUSE_DATABASE}_${RANDOM}"

cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS values_fallback" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS values_decimal" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS values_decimal_retry" 2>/dev/null
}

wait_for_flush_query_id()
{
    local query_id=$1
    local flush_query_id

    for _ in {1..60}; do
        $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
        flush_query_id=$($CLICKHOUSE_CLIENT -q "
            SELECT flush_query_id
            FROM system.asynchronous_insert_log
            WHERE query_id = '${query_id}'
            ORDER BY event_time_microseconds DESC
            LIMIT 1")
        if [ -n "$flush_query_id" ]; then
            echo "$flush_query_id"
            return
        fi
        sleep 0.5
    done

    echo "Failed to find the asynchronous insert flush query for ${query_id}" >&2
    return 1
}

trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE values_fallback
    (
        id UInt32,
        target_ids Array(String),
        target_tags Map(String, Array(String)),
        raw_json_as_string String,
        raw_json_as_map Map(String, String),
        activity_type Enum8('ADMINISTRATION' = 1, 'OTHER' = 2)
    )
    ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE values_decimal (x Decimal32(2)) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE values_decimal_retry (x Decimal32(2)) ENGINE = Memory"

${CLICKHOUSE_CURL} -sS \
    "${CLICKHOUSE_URL}&query_id=${fallback_query_id}&async_insert=1&wait_for_async_insert=1" \
    --data-binary "
        INSERT INTO values_fallback (id, target_tags, raw_json_as_string, raw_json_as_map, activity_type)
        VALUES (1 + 0, {'3f37a58df98ba6e2': ['aaaa']}, '{}', {}, 'ADMINISTRATION'),
               (+2, {}, '{}', {}, 'OTHER'),
               ((SELECT 3), {}, '{}', {}, 'OTHER')" > /dev/null

$CLICKHOUSE_CLIENT -q "SELECT id, target_ids, target_tags, raw_json_as_string, raw_json_as_map, activity_type FROM values_fallback ORDER BY id"

flush_query_id=$(wait_for_flush_query_id "$fallback_query_id")
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS error_log"
$CLICKHOUSE_CLIENT -q "SELECT count() = 0 FROM system.errors WHERE query_id = '${flush_query_id}'"
$CLICKHOUSE_CLIENT -q "SELECT count() = 0 FROM system.error_log WHERE last_error_query_id = '${flush_query_id}'"

${CLICKHOUSE_CURL} -sS \
    "${CLICKHOUSE_URL}&query_id=${overflow_query_id}&async_insert=1&wait_for_async_insert=1" \
    --data-binary "INSERT INTO values_decimal VALUES (12345678.91)" > /dev/null

overflow_flush_query_id=$(wait_for_flush_query_id "$overflow_query_id")
$CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM system.errors WHERE name = 'ARGUMENT_OUT_OF_BOUND' AND query_id = '${overflow_flush_query_id}'"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS error_log"
$CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM system.error_log WHERE error = 'ARGUMENT_OUT_OF_BOUND' AND last_error_query_id = '${overflow_flush_query_id}'"

${CLICKHOUSE_CURL} -sS \
    "${CLICKHOUSE_URL}&query_id=${overflow_retry_query_id}&async_insert=1&wait_for_async_insert=1&input_format_values_deduce_templates_of_expressions=0" \
    --data-binary "INSERT INTO values_decimal_retry VALUES (1.20 + 0.03), (12345678.91)" > /dev/null

overflow_retry_flush_query_id=$(wait_for_flush_query_id "$overflow_retry_query_id")
$CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM system.errors WHERE name = 'ARGUMENT_OUT_OF_BOUND' AND query_id = '${overflow_retry_flush_query_id}'"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS error_log"
$CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM system.error_log WHERE error = 'ARGUMENT_OUT_OF_BOUND' AND last_error_query_id = '${overflow_retry_flush_query_id}'"
