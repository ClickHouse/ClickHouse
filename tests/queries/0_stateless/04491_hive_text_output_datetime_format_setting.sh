#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# HiveText timestamps must always be the simple `yyyy-MM-dd HH:mm:ss[.fff]` text that Hive parses
# as TIMESTAMP, regardless of the session/server `date_time_output_format`. A non-default value of
# that setting (`unix_timestamp` emits epoch seconds, `iso` emits `T...Z`) must NOT leak into the
# HiveText output. A visible fields delimiter keeps the reference readable.
for fmt in simple unix_timestamp iso; do
    ${CLICKHOUSE_CLIENT} --query "SELECT
        toDateTime('2024-04-09 12:34:56', 'UTC') AS dt,
        toDateTime64('2024-04-09 12:34:56.789', 3, 'UTC') AS dt64
        FORMAT HiveText
        SETTINGS date_time_output_format = '${fmt}', input_format_hive_text_fields_delimiter = ','"
done
