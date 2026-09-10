#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `format` / `output_format` / `default_format` settings (also set by the `--format` /
# `--output-format` command-line options) now select the output format on the native client too,
# matching the server-side precedence (`resolveOutputFormatName`): `output_format` > `format` >
# query `FORMAT` clause > `default_format` > the client default.

echo "-- output_format setting selects the output format"
${CLICKHOUSE_CLIENT} --query "SELECT 1 AS x SETTINGS output_format = 'JSONEachRow'"

echo "-- format setting selects the output format too"
${CLICKHOUSE_CLIENT} --query "SELECT 2 AS x SETTINGS format = 'CSV'"

echo "-- default_format setting is the fallback when there is no FORMAT clause / override"
${CLICKHOUSE_CLIENT} --query "SELECT 3 AS x SETTINGS default_format = 'CSVWithNames'"

echo "-- output_format wins over the query FORMAT clause (matches the server)"
${CLICKHOUSE_CLIENT} --query "SELECT 4 AS x FORMAT TSV SETTINGS output_format = 'JSONEachRow'"

echo "-- a query FORMAT clause is respected when no format setting is given"
${CLICKHOUSE_CLIENT} --query "SELECT 5 AS x FORMAT CSV"

echo "-- the --output-format command-line option maps to the output_format setting"
${CLICKHOUSE_CLIENT} --output-format JSONEachRow --query "SELECT 6 AS x"
