#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings
# no-random-settings: this test asserts a specific parse-time error
# (CANNOT_PARSE_INPUT_ASSERTION_FAILED). Randomized session settings injected by the
# flaky-check runner (e.g. malformed session_timezone) can produce an unrelated
# BAD_ARGUMENTS error during INSERT FROM INFILE setup, masking the targeted assertion.
# Test: covers `input_format_hive_text_allow_variable_number_of_columns` setting wiring through
#       `updateFormatSettings` in `src/Processors/Formats/Impl/HiveTextRowInputFormat.cpp` to
#       `csv.allow_variable_number_of_columns` consumed at
#       `src/Processors/Formats/RowInputFormatWithNamesAndTypes.cpp:244` and lines 285-293.
# PR test only covers the default `=1` case with row that has fewer fields than the table.
# This test covers:
#   (1) `=0` strict mode — must error out when row has fewer fields (back-compat path).
#   (2) `=1` with row having MORE fields than the table — extras must be skipped (PR description path).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Use per-invocation unique paths in $CLICKHOUSE_TMP. The flaky-check runner
# launches this test multiple times concurrently; sharing a fixed path under
# data_hive/ races on file write/delete and produces non-deterministic INSERT
# behaviour (file truncated mid-read, or removed before the second invocation
# starts). $CLICKHOUSE_TEST_UNIQUE_NAME embeds the random database name and is
# unique per test invocation, so each parallel run gets its own files.
DATA_FEWER="${CLICKHOUSE_TMP}/03999_hive_fewer_fields_${CLICKHOUSE_TEST_UNIQUE_NAME}.txt"
DATA_EXTRAS="${CLICKHOUSE_TMP}/03999_hive_extra_fields_${CLICKHOUSE_TEST_UNIQUE_NAME}.txt"

printf '1,3\n3,5,9' > "${DATA_FEWER}"
printf '1,2,3,4,5\n6,7,8\n' > "${DATA_EXTRAS}"

# (1) setting=0 → strict CSV path (`else` branch at line 295 in RowInputFormatWithNamesAndTypes.cpp).
# Row with fewer fields than table columns must produce a parse error.
$CLICKHOUSE_CLIENT -q "drop table if exists test_hive_strict"
$CLICKHOUSE_CLIENT -q "create table test_hive_strict (a UInt16, b UInt32, c UInt32) engine=MergeTree order by a"
$CLICKHOUSE_CLIENT -q "insert into test_hive_strict from infile '${DATA_FEWER}' SETTINGS input_format_hive_text_fields_delimiter=',', input_format_hive_text_allow_variable_number_of_columns=0 FORMAT HIVETEXT" 2>&1 | grep -c -F "CANNOT_PARSE_INPUT_ASSERTION_FAILED"
$CLICKHOUSE_CLIENT -q "select count() from test_hive_strict"
$CLICKHOUSE_CLIENT -q "drop table test_hive_strict"

# (2) setting=1 (default) with extras → trailing fields are skipped (line 285-293).
$CLICKHOUSE_CLIENT -q "drop table if exists test_hive_extras"
$CLICKHOUSE_CLIENT -q "create table test_hive_extras (a UInt16, b UInt32, c UInt32) engine=MergeTree order by a"
$CLICKHOUSE_CLIENT -q "insert into test_hive_extras from infile '${DATA_EXTRAS}' SETTINGS input_format_hive_text_fields_delimiter=',' FORMAT HIVETEXT"
$CLICKHOUSE_CLIENT -q "select * from test_hive_extras order by a"
$CLICKHOUSE_CLIENT -q "drop table test_hive_extras"

rm -f "${DATA_FEWER}" "${DATA_EXTRAS}"
