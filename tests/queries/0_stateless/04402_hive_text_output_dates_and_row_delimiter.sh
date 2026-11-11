#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Date and DateTime types must be written as Hive date/timestamp strings (delegating to the
# default text serialization), not as the underlying numeric values. The output format is
# pinned with date_time_output_format = 'simple' so the rendered representation is explicit.
${CLICKHOUSE_CLIENT} --query "SELECT
    toDate('2024-04-09') AS d,
    toDate32('2024-04-09') AS d32,
    toDateTime('2024-04-09 12:34:56', 'UTC') AS dt,
    toDateTime64('2024-04-09 12:34:56.789', 3, 'UTC') AS dt64
    FORMAT HiveText SETTINGS date_time_output_format = 'simple'"

# The rows delimiter is configurable via format_hive_text_rows_delimiter and is written after
# every row, including the last one. Here a visible ';' is used between three rows.
${CLICKHOUSE_CLIENT} --query "SELECT number FROM numbers(3) FORMAT HiveText SETTINGS format_hive_text_rows_delimiter = ';'"
echo
