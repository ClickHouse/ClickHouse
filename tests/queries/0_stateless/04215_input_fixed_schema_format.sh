#!/usr/bin/env bash

# Tests that the `input` table function infers its structure from the surrounding
# `INSERT` query's `FORMAT` when that format has a fixed schema (e.g. `LineAsString`,
# `RawBLOB`, `JSONAsString`). For these formats the user no longer has to repeat the
# structure as `input('line String')` etc.
#
# See https://github.com/ClickHouse/ClickHouse/issues/104532

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t_input_line_as_string"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t_input_line_as_string_wide"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t_input_raw_blob"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t_input_json_as_string"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t_input_csv"

# 1. LineAsString — the original issue from #104532. The `input()` call has no structure,
#    but the format is fixed-schema and provides `line String`.
${CLICKHOUSE_CLIENT} --query="CREATE TABLE t_input_line_as_string (line String) ENGINE = Memory"
printf 'one\ntwo\nthree\n' | \
    ${CLICKHOUSE_CLIENT} --query="INSERT INTO t_input_line_as_string SELECT line FROM input() FORMAT LineAsString"
${CLICKHOUSE_CLIENT} --query="SELECT count(), groupArray(line) FROM t_input_line_as_string"

# 2. LineAsString with extra constant columns derived in the SELECT list. This is the
#    exact shape from the issue: the destination has columns the input format does not
#    provide, so they are filled in by literals around the inferred `line` reference.
${CLICKHOUSE_CLIENT} --query="CREATE TABLE t_input_line_as_string_wide (system String, repo String, line String) ENGINE = Memory"
printf 'log-a\nlog-b\n' | \
    ${CLICKHOUSE_CLIENT} --query="INSERT INTO t_input_line_as_string_wide SELECT '@system@' AS system, '@repo@' AS repo, line FROM input() FORMAT LineAsString"
${CLICKHOUSE_CLIENT} --query="SELECT count(), groupArray((system, repo, line)) FROM t_input_line_as_string_wide"

# 3. RawBLOB — fixed-schema format with column name `raw_blob`. Single value per request.
${CLICKHOUSE_CLIENT} --query="CREATE TABLE t_input_raw_blob (data String) ENGINE = Memory"
printf 'binary content here' | \
    ${CLICKHOUSE_CLIENT} --query="INSERT INTO t_input_raw_blob SELECT raw_blob FROM input() FORMAT RawBLOB"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM t_input_raw_blob"

# 4. JSONAsString — fixed-schema format with column name `json`.
${CLICKHOUSE_CLIENT} --query="CREATE TABLE t_input_json_as_string (json String) ENGINE = Memory"
printf '{"a": 1}\n{"b": 2}\n' | \
    ${CLICKHOUSE_CLIENT} --query="INSERT INTO t_input_json_as_string SELECT json FROM input() FORMAT JSONAsString"
${CLICKHOUSE_CLIENT} --query="SELECT count(), arraySort(groupArray(json)) FROM t_input_json_as_string"

# 5. Negative case: a format with no fixed schema (`CSV`). The user must still provide
#    a structure manually; we should preserve the old, actionable error message.
#    Capture the client's exit status explicitly so the test fails loudly if the
#    `INSERT` unexpectedly succeeds instead of silently producing an empty line that
#    would only be caught by the reference diff.
${CLICKHOUSE_CLIENT} --query="CREATE TABLE t_input_csv (a Int32, b Int32) ENGINE = Memory"
set +e
csv_output=$(printf '1,2\n' | ${CLICKHOUSE_CLIENT} --query="INSERT INTO t_input_csv SELECT a, b FROM input() FORMAT CSV" 2>&1)
csv_status=$?
set -e
if [ "$csv_status" -eq 0 ]; then
    echo "ERROR: INSERT FROM input() FORMAT CSV was expected to fail but succeeded; output: $csv_output"
    exit 1
fi
echo "$csv_output" | grep -oF 'CANNOT_EXTRACT_TABLE_STRUCTURE' | head -n 1

# 6. Existing behaviour: explicit structure on `input(...)` still works, including when
#    its column name does not match the format's fixed schema.
${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE t_input_line_as_string"
printf 'still\nworks\n' | \
    ${CLICKHOUSE_CLIENT} --query="INSERT INTO t_input_line_as_string SELECT my_col FROM input('my_col String') FORMAT LineAsString"
${CLICKHOUSE_CLIENT} --query="SELECT count(), groupArray(line) FROM t_input_line_as_string"

${CLICKHOUSE_CLIENT} --query="DROP TABLE t_input_line_as_string"
${CLICKHOUSE_CLIENT} --query="DROP TABLE t_input_line_as_string_wide"
${CLICKHOUSE_CLIENT} --query="DROP TABLE t_input_raw_blob"
${CLICKHOUSE_CLIENT} --query="DROP TABLE t_input_json_as_string"
${CLICKHOUSE_CLIENT} --query="DROP TABLE t_input_csv"
