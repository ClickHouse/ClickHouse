#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A custom setting (enabled by `custom_settings_prefixes`) can hold an AST rather than a literal, e.g.
# `custom_x = disk(type = 's3', secret_access_key = '...')`. That value is a `CustomType` `Field`, so
# no plain `Field` formatter hides the credential; only `CustomType::toString(show_secrets)` does.
# `custom_x` only exists for the lifetime of an HTTP session, so every case below sets it with
# `SET custom_x = ...` inside a session and reuses that `session_id` for the query that follows.
#
# Each case uses its own canary string, so a leak points straight at the sink that leaked it.

SETTINGS_CANARY="c05063settingscolumn"
OTEL_CANARY="c05063opentelemetry"
DDL_CANARY="c05063distributedddl"
QUERYLOG_CANARY="c05063querylog"

# 1. `system.settings.value`.
SESSION_SETTINGS="05063_settings_$CLICKHOUSE_DATABASE"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=$SESSION_SETTINGS" \
    --data-binary "SET custom_x = disk(type = 's3', secret_access_key = '$SETTINGS_CANARY')" > /dev/null
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=$SESSION_SETTINGS" \
    --data-binary "SELECT
        position(value, '[HIDDEN]') > 0,
        position(value, '$SETTINGS_CANARY') = 0
    FROM system.settings WHERE name = 'custom_x'"

# 2. `system.opentelemetry_span_log`, attribute `clickhouse.setting.custom_x`.
SESSION_OTEL="05063_otel_$CLICKHOUSE_DATABASE"
QUERY_ID_OTEL="05063_otel_query_$CLICKHOUSE_DATABASE"
TRACE_ID=$($CLICKHOUSE_CLIENT -q "SELECT lower(hex(generateUUIDv4()))")
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=$SESSION_OTEL" \
    --data-binary "SET custom_x = disk(type = 's3', secret_access_key = '$OTEL_CANARY')" > /dev/null
${CLICKHOUSE_CURL} -sS \
    -H "traceparent: 00-${TRACE_ID}-0000000000000010-01" \
    "${CLICKHOUSE_URL}&session_id=$SESSION_OTEL&query_id=$QUERY_ID_OTEL&log_query_settings=1" \
    --data-binary "SELECT 1" > /dev/null

for _ in {1..60}; do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS opentelemetry_span_log"
    OTEL_RESULT=$($CLICKHOUSE_CLIENT -q "SELECT
            position(attribute['clickhouse.setting.custom_x'], '[HIDDEN]') > 0,
            position(attribute['clickhouse.setting.custom_x'], '$OTEL_CANARY') = 0
        FROM system.opentelemetry_span_log
        WHERE operation_name = 'query' AND attribute['clickhouse.query_id'] = '$QUERY_ID_OTEL'
            AND attribute['clickhouse.setting.custom_x'] != ''")
    [ -n "$OTEL_RESULT" ] && break
    sleep 0.5
done
echo "$OTEL_RESULT"

# 3. `system.distributed_ddl_queue.settings`.
SESSION_DDL="05063_ddl_$CLICKHOUSE_DATABASE"
DDL_DATABASE="${CLICKHOUSE_DATABASE}_05063_ddl"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=$SESSION_DDL" \
    --data-binary "SET custom_x = disk(type = 's3', secret_access_key = '$DDL_CANARY')" > /dev/null
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=$SESSION_DDL&distributed_ddl_output_mode=none" \
    --data-binary "CREATE DATABASE IF NOT EXISTS $DDL_DATABASE ON CLUSTER test_shard_localhost" > /dev/null

$CLICKHOUSE_CLIENT -q "SELECT
        countIf(position(settings['custom_x'], '[HIDDEN]') > 0) > 0,
        countIf(position(settings['custom_x'], '$DDL_CANARY') > 0) = 0
    FROM system.distributed_ddl_queue
    WHERE position(query, '$DDL_DATABASE') > 0"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $DDL_DATABASE ON CLUSTER test_shard_localhost" > /dev/null

# 4. `system.query_log.Settings`. This one already passes and guards against regressing it.
SESSION_QUERYLOG="05063_querylog_$CLICKHOUSE_DATABASE"
QUERY_ID_QUERYLOG="05063_querylog_query_$CLICKHOUSE_DATABASE"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=$SESSION_QUERYLOG" \
    --data-binary "SET custom_x = disk(type = 's3', secret_access_key = '$QUERYLOG_CANARY')" > /dev/null
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=$SESSION_QUERYLOG&query_id=$QUERY_ID_QUERYLOG&log_queries=1" \
    --data-binary "SELECT 1" > /dev/null

for _ in {1..60}; do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    QUERYLOG_RESULT=$($CLICKHOUSE_CLIENT -q "SELECT
            position(Settings['custom_x'], '[HIDDEN]') > 0,
            position(Settings['custom_x'], '$QUERYLOG_CANARY') = 0
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '$QUERY_ID_QUERYLOG' AND type = 'QueryFinish'")
    [ -n "$QUERYLOG_RESULT" ] && break
    sleep 0.5
done
echo "$QUERYLOG_RESULT"
