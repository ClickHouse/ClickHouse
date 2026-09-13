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
trap 'rm -f "${OPTOUT_FILE}" "${FLAT_FILE}" "${WNT_FILE}" "${CSNT_FILE}"' EXIT

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

SELECT '-- non-null Nullable(Tuple(...)): value flattens, so the header flattens the inner tuple too';
SELECT materialize(CAST((1, 'a'), 'Nullable(Tuple(ID UInt64, Name String))')) AS User
FORMAT CSVWithNamesAndTypes
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
