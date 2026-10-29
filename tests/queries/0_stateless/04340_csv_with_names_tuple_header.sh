#!/usr/bin/env bash
# Issue #107342: CSVWithNames header must flatten named tuples to match the flattened data width.
# clickhouse-local with unique file names so it runs in parallel with itself (no no-parallel tag).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cd "${CLICKHOUSE_TMP}" || exit 1

OPTOUT_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}_optout.csv"
FLAT_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}_flat.csv"
WNT_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}_wnt.csv"
CSNT_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}_csnt.csv"
NTUP_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}_ntup.csv"
trap 'rm -f "${OPTOUT_FILE}" "${FLAT_FILE}" "${WNT_FILE}" "${CSNT_FILE}" "${NTUP_FILE}"' EXIT

${CLICKHOUSE_LOCAL} --multiquery "
SELECT '-- CSVWithNames: header flattened to match data (default)';
SELECT
    (123::UInt64, 'Sophia')::Tuple(ID UInt64, Name String) AS User,
    (1::UInt16, 'Net1')::Tuple(ID UInt64, Name String) AS Network
FORMAT CSVWithNames;

SELECT '-- CSVWithNamesAndTypes: names and types rows both flattened (default)';
SELECT (1::UInt64, 'a')::Tuple(ID UInt64, Name String) AS User
FORMAT CSVWithNamesAndTypes;

SELECT '-- opt-out: keep the single top-level tuple name and type';
SELECT (1::UInt64, 'a')::Tuple(ID UInt64, Name String) AS User
FORMAT CSVWithNamesAndTypes
SETTINGS output_format_csv_header_serialize_tuple_into_separate_columns = 0;

SELECT '-- not flattened when values are not flattened either';
SELECT (1::UInt64, 'a')::Tuple(ID UInt64, Name String) AS User
FORMAT CSVWithNames
SETTINGS output_format_csv_serialize_tuple_into_separate_columns = 0;

SELECT '-- nested tuples: recursive dotted names';
SELECT ((1::UInt8, (2::UInt8, 3::UInt8)), 'b')::Tuple(A Tuple(X UInt8, Y Tuple(P UInt8, Q UInt8)), B String) AS T
FORMAT CSVWithNames;

SELECT '-- unnamed tuple: positional element names';
SELECT (1::UInt8, 'a')::Tuple(UInt8, String) AS U
FORMAT CSVWithNames;

SELECT '-- mixed plain and tuple columns';
SELECT 5::UInt8 AS x, (1::UInt8, 'a')::Tuple(ID UInt8, Name String) AS U, 'z' AS y
FORMAT CSVWithNames;

SELECT '-- no tuples: header unchanged';
SELECT 1::UInt8 AS a, 'x' AS b
FORMAT CSVWithNames;

SELECT '-- empty tuple in a mixed row: still occupies one field, header keeps one cell';
SELECT 1::UInt8 AS x, tuple()::Tuple() AS t, 2::UInt8 AS y
FORMAT CSVWithNamesAndTypes;

SELECT '-- Nullable(Tuple(...)): kept as a single CSV field (NOT flattened) so NULL and non-null rows have the same width (issue #107342)';
SELECT materialize(if(number % 2, CAST(NULL, 'Nullable(Tuple(ID UInt64, Name String))'), CAST((1, 'a'), 'Nullable(Tuple(ID UInt64, Name String))'))) AS User
FROM numbers(4)
FORMAT CSVWithNamesAndTypes
SETTINGS allow_experimental_nullable_tuple_type = 1;

SELECT '-- nested Nullable(Tuple(..., Tuple(...))): still one CSV field';
SELECT materialize(CAST((1, (2, 3)), 'Nullable(Tuple(a UInt8, b Tuple(c UInt8, d UInt8)))')) AS t
FORMAT CSVWithNames
SETTINGS allow_experimental_nullable_tuple_type = 1;

SELECT '-- scalar Nullable column: stays one cell with the full Nullable type name';
SELECT CAST(1, 'Nullable(UInt64)') AS n, 'x' AS s
FORMAT CSVWithNamesAndTypes;

SELECT '-- CustomSeparatedWithNames, CSV rule, comma field delimiter: header flattened (delimiters match)';
SELECT 5::UInt8 AS x, (1::UInt64, 'a')::Tuple(ID UInt64, Name String) AS User
FORMAT CustomSeparatedWithNames
SETTINGS format_custom_escaping_rule = 'CSV', format_custom_field_delimiter = ',';

SELECT '-- CustomSeparatedWithNamesAndTypes, CSV rule, comma field delimiter: names and types flattened';
SELECT (1::UInt64, 'a')::Tuple(ID UInt64, Name String) AS User
FORMAT CustomSeparatedWithNamesAndTypes
SETTINGS format_custom_escaping_rule = 'CSV', format_custom_field_delimiter = ',';

SELECT '-- CustomSeparatedWithNames, CSV rule, non-comma (|) field delimiter: header NOT flattened (matches one-field tuple value)';
SELECT 5::UInt8 AS x, (1::UInt64, 'a')::Tuple(ID UInt64, Name String) AS User
FORMAT CustomSeparatedWithNames
SETTINGS format_custom_escaping_rule = 'CSV', format_custom_field_delimiter = '|';

SELECT '-- CustomSeparatedWithNames, CSV rule, default tab field delimiter: header NOT flattened';
SELECT 5::UInt8 AS x, (1::UInt64, 'a')::Tuple(ID UInt64, Name String) AS User
FORMAT CustomSeparatedWithNames
SETTINGS format_custom_escaping_rule = 'CSV';

SELECT '-- CustomSeparatedWithNames with non-CSV escaping rule: header not flattened';
SELECT (1::UInt64, 'a')::Tuple(ID UInt64, Name String) AS User
FORMAT CustomSeparatedWithNames
SETTINGS format_custom_escaping_rule = 'Quoted', format_custom_field_delimiter = ',';
"

# Round-trip: data written with a flattened header reads back into the tuple positionally
# (input_format_with_names_use_header = 0), and the opt-out single-name header reads back by name.
echo '-- round-trip: opt-out single-name header reads back by name'
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${OPTOUT_FILE}', CSVWithNames)
SELECT (1::UInt64, 'a')::Tuple(ID UInt64, Name String) AS User
SETTINGS engine_file_truncate_on_insert = 1, output_format_csv_header_serialize_tuple_into_separate_columns = 0"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${OPTOUT_FILE}', CSVWithNames, 'User Tuple(ID UInt64, Name String)')"

echo '-- round-trip: flattened header reads back positionally (use_header = 0)'
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${FLAT_FILE}', CSVWithNames)
SELECT (1::UInt64, 'a')::Tuple(ID UInt64, Name String) AS User
SETTINGS engine_file_truncate_on_insert = 1"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${FLAT_FILE}', CSVWithNames, 'User Tuple(ID UInt64, Name String)')
SETTINGS input_format_with_names_use_header = 0"

# CSVWithNamesAndTypes also flattens the types row, so the positional round-trip additionally
# needs input_format_with_types_use_header = 0 (otherwise readPrefix validates the flattened
# types row against the single top-level Tuple input field and rejects it).
echo '-- round-trip: CSVWithNamesAndTypes flattened header+types read back positionally (both use_header = 0)'
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${WNT_FILE}', CSVWithNamesAndTypes)
SELECT (1::UInt64, 'a')::Tuple(ID UInt64, Name String) AS User
SETTINGS engine_file_truncate_on_insert = 1"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${WNT_FILE}', CSVWithNamesAndTypes, 'User Tuple(ID UInt64, Name String)')
SETTINGS input_format_with_names_use_header = 0, input_format_with_types_use_header = 0"

# CustomSeparatedWithNamesAndTypes (CSV rule, matching field/CSV delimiter) flattens the types row
# the same way, so its positional round-trip also needs input_format_with_types_use_header = 0.
echo '-- round-trip: CustomSeparatedWithNamesAndTypes flattened header+types read back positionally (both use_header = 0)'
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${CSNT_FILE}', CustomSeparatedWithNamesAndTypes)
SELECT (1::UInt64, 'a')::Tuple(ID UInt64, Name String) AS User
SETTINGS engine_file_truncate_on_insert = 1, format_custom_escaping_rule = 'CSV', format_custom_field_delimiter = ','"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${CSNT_FILE}', CustomSeparatedWithNamesAndTypes, 'User Tuple(ID UInt64, Name String)')
SETTINGS format_custom_escaping_rule = 'CSV', format_custom_field_delimiter = ',', input_format_with_names_use_header = 0, input_format_with_types_use_header = 0"

# Nullable(Tuple) is a single CSV field (one header cell), so a file containing NULL and non-null
# rows reads back by name without a width mismatch (issue #107342: previously the flattened header
# desynced from the single \N field of a NULL row).
echo '-- round-trip: Nullable(Tuple) with NULLs reads back by name'
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${NTUP_FILE}', CSVWithNames)
SELECT materialize(if(number % 2, CAST(NULL, 'Nullable(Tuple(ID UInt64, Name String))'), CAST((1, 'a'), 'Nullable(Tuple(ID UInt64, Name String))'))) AS User
FROM numbers(4)
SETTINGS engine_file_truncate_on_insert = 1, allow_experimental_nullable_tuple_type = 1"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${NTUP_FILE}', CSVWithNames, 'User Nullable(Tuple(ID UInt64, Name String))')
SETTINGS allow_experimental_nullable_tuple_type = 1"

# A Nullable(Tuple()) value is written as a single quoted field ("()"), so a genuinely empty
# input field is no longer a valid tuple value: with input_format_csv_empty_as_default = 1
# (the default) it is the nullable default (NULL), like every other Nullable type. The quoted
# "()" / "(5)" fields still parse into the empty / non-empty tuple value, so it is not always NULL.
echo '-- empty input field into Nullable(Tuple()): empty_as_default=1 (default) gives NULL, "()" parses'
printf '\n"()"\n' | ${CLICKHOUSE_LOCAL} --query "SELECT c0 FROM file('/dev/stdin', CSV, 'c0 Nullable(Tuple())')
SETTINGS allow_experimental_nullable_tuple_type = 1"
echo '-- empty input field into Nullable(Tuple(Int32)): NULL, "(5)" parses'
printf '\n"(5)"\n' | ${CLICKHOUSE_LOCAL} --query "SELECT c0 FROM file('/dev/stdin', CSV, 'c0 Nullable(Tuple(a Int32))')
SETTINGS allow_experimental_nullable_tuple_type = 1"

# Nullable(Tuple) is written as a single CSV field and is read back only from a single field,
# never from separate columns, regardless of input_format_csv_deserialize_separate_columns_into_tuple
# (separate-columns parsing is unsupported for Nullable(Tuple) because a leading \N is ambiguous).
echo '-- input: new single-field encoding reads (with NULL rows), setting = 1 (default)'
printf '"(1,\x27a\x27)"\n\\N\n"(2,\x27b\x27)"\n' | ${CLICKHOUSE_LOCAL} --query "SELECT c1 FROM file('/dev/stdin', CSV, 'c1 Nullable(Tuple(ID UInt64, Name String))')
SETTINGS allow_experimental_nullable_tuple_type = 1"
echo '-- input: single-field encoding reads the same with the separate-columns setting = 0'
printf '"(3,\x27c\x27)"\n\\N\n' | ${CLICKHOUSE_LOCAL} --query "SELECT c1 FROM file('/dev/stdin', CSV, 'c1 Nullable(Tuple(ID UInt64, Name String))')
SETTINGS allow_experimental_nullable_tuple_type = 1, input_format_csv_deserialize_separate_columns_into_tuple = 0"
