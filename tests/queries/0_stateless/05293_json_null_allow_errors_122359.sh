#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/122359
# With `input_format_null_as_default = 0`, a JSON `null` in a non-`Nullable` numeric column is a bad value of this row:
# `input_format_allow_errors_num` / `input_format_allow_errors_ratio` skip it (as for a `String` column), and the error reports the row.

SETTINGS="SETTINGS input_format_null_as_default = 0"

$CLICKHOUSE_CLIENT -q "
SELECT 'JSONEachRow Int64 num';
SELECT * FROM format(JSONEachRow, 'a Int64', '{\"a\": 1}\n{\"a\": null}\n{\"a\": 3}') $SETTINGS, input_format_allow_errors_num = 1;
SELECT 'JSONEachRow Float64 ratio';
SELECT * FROM format(JSONEachRow, 'a Float64', '{\"a\": 1.5}\n{\"a\": null}\n{\"a\": 3.5}') $SETTINGS, input_format_allow_errors_ratio = 0.5;
SELECT 'JSONEachRow Array(Int64) num';
SELECT * FROM format(JSONEachRow, 'a Array(Int64)', '{\"a\": [1]}\n{\"a\": [2, null]}\n{\"a\": [3]}') $SETTINGS, input_format_allow_errors_num = 1;
SELECT 'JSONCompactEachRow Int64 num';
SELECT * FROM format(JSONCompactEachRow, 'a Int64, b UInt8', '[1, 1]\n[null, 2]\n[3, 3]') $SETTINGS, input_format_allow_errors_num = 1;
SELECT 'JSONCompactEachRow Float64 ratio';
SELECT * FROM format(JSONCompactEachRow, 'a Float64, b UInt8', '[1.5, 1]\n[null, 2]\n[3.5, 3]') $SETTINGS, input_format_allow_errors_ratio = 0.5;
SELECT 'CustomSeparated Int64 num';
SELECT * FROM format(CustomSeparated, 'a Int64, b UInt8', '1\t1\nnull\t2\n3\t3\n') $SETTINGS, input_format_allow_errors_num = 1, format_custom_escaping_rule = 'JSON';
SELECT 'CustomSeparated Float64 ratio';
SELECT * FROM format(CustomSeparated, 'a Float64, b UInt8', '1.5\t1\nnull\t2\n3.5\t3\n') $SETTINGS, input_format_allow_errors_ratio = 0.5, format_custom_escaping_rule = 'JSON';
SELECT 'Nullable(Int64) keeps NULL';
SELECT * FROM format(JSONEachRow, 'a Nullable(Int64)', '{\"a\": 1}\n{\"a\": null}\n{\"a\": 3}') $SETTINGS, input_format_allow_errors_num = 1;
"

# Without allowed errors, or with too many of them, the query still fails, and the error has the row number.
echo 'JSONEachRow Int64 no allowed errors'
$CLICKHOUSE_CLIENT -q "SELECT * FROM format(JSONEachRow, 'a Int64', '{\"a\": 1}\n{\"a\": null}\n{\"a\": 3}') $SETTINGS, input_format_allow_errors_num = 0" 2>&1 \
    | grep -o -e '(at row [0-9]*)' -e 'CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN' | LC_ALL=C sort -u
echo 'JSONCompactEachRow Float64 no allowed errors'
$CLICKHOUSE_CLIENT -q "SELECT * FROM format(JSONCompactEachRow, 'a Float64', '[1.5]\n[null]') $SETTINGS" 2>&1 \
    | grep -o -e '(at row [0-9]*)' -e 'CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN' | LC_ALL=C sort -u
echo 'CustomSeparated Int64 no allowed errors'
$CLICKHOUSE_CLIENT -q "SELECT * FROM format(CustomSeparated, 'a Int64', '1\nnull\n') $SETTINGS, format_custom_escaping_rule = 'JSON'" 2>&1 \
    | grep -o -e '(at row [0-9]*)' -e 'CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN' | LC_ALL=C sort -u
echo 'JSONEachRow Int64 too many errors'
$CLICKHOUSE_CLIENT -q "SELECT * FROM format(JSONEachRow, 'a Int64', '{\"a\": 1}\n{\"a\": null}\n{\"a\": null}') $SETTINGS, input_format_allow_errors_num = 1" 2>&1 \
    | grep -o -e '(at row [0-9]*)' -e 'Already have [0-9]* errors' -e 'CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN' | LC_ALL=C sort -u
