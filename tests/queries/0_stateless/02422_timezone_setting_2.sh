#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=123123123123"  -d "SET force_timezone='Europe/Helsinki'";

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=123123123123" -d "SELECT toDateTime('1999-12-12 12:12:12') - toDateTime('1999-12-12 12:12:12', 'Europe/Moscow')";
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=123123123123&force_timezone=Asia/Novosibirsk" -d "SELECT toUnixTimestamp(toDateTime('1999-12-12 12:12:12'))";
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=123123123123" -d "SELECT toUnixTimestamp(toDateTime('1999-12-12 12:12:12'))";
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=123123123123&force_timezone=America/Denver" -d "SELECT toStartOfHour(toDateTime('1999-12-12 12:12:12'), 'UTC')";
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=123123123123" -d "SELECT toUnixTimestamp64Milli(toDateTime64('1999-12-12 12:12:12.123', 3))";
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=123123123123&force_timezone=Europe/Zurich" -d "SELECT toUnixTimestamp64Milli(toDateTime64('1999-12-12 12:12:12.123', 3))";
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=123123123123&force_timezone=Europe/Zurich" -d "SELECT toTimeZone(toDateTime64('1999-12-12 12:12:12.123', 3) + toIntervalSecond(5), 'UTC')";