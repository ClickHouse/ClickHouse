#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

# Copy CSV test files
cp "$CURDIR"/data_csv/csv_with_utf8-bom.csv $USER_FILES_PATH/
cp "$CURDIR"/data_csv/csv_with_utf16le-bom.csv $USER_FILES_PATH/
cp "$CURDIR"/data_csv/csv_with_utf16be-bom.csv $USER_FILES_PATH/
cp "$CURDIR"/data_csv/csv_with_utf32le-bom.csv $USER_FILES_PATH/
cp "$CURDIR"/data_csv/csv_with_utf32be-bom.csv $USER_FILES_PATH/

# Copy TSV test files
cp "$CURDIR"/data_csv/tsv_with_utf16le-bom.tsv $USER_FILES_PATH/
cp "$CURDIR"/data_csv/tsv_with_utf16be-bom.tsv $USER_FILES_PATH/
cp "$CURDIR"/data_csv/tsv_with_utf32le-bom.tsv $USER_FILES_PATH/
cp "$CURDIR"/data_csv/tsv_with_utf32be-bom.tsv $USER_FILES_PATH/

# Copy JSON test files
cp "$CURDIR"/data_csv/json_with_utf16le-bom.jsonl $USER_FILES_PATH/
cp "$CURDIR"/data_csv/json_with_utf16be-bom.jsonl $USER_FILES_PATH/
cp "$CURDIR"/data_csv/json_with_utf32le-bom.jsonl $USER_FILES_PATH/
cp "$CURDIR"/data_csv/json_with_utf32be-bom.jsonl $USER_FILES_PATH/

# Copy JSONColumns test files
cp "$CURDIR"/data_csv/jsoncolumns_with_utf16le-bom.json $USER_FILES_PATH/
cp "$CURDIR"/data_csv/jsoncolumns_with_utf16be-bom.json $USER_FILES_PATH/

# Copy JSONCompactColumns test files
cp "$CURDIR"/data_csv/jsoncompactcolumns_with_utf16le-bom.json $USER_FILES_PATH/
cp "$CURDIR"/data_csv/jsoncompactcolumns_with_utf16be-bom.json $USER_FILES_PATH/

# Test CSV format
${CLICKHOUSE_CLIENT} --query="SELECT 'CSV UTF-8 BOM:'"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM file('csv_with_utf8-bom.csv', CSVWithNames) ORDER BY id"

${CLICKHOUSE_CLIENT} --query="SELECT 'CSV UTF-16 LE BOM:'"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM file('csv_with_utf16le-bom.csv', CSVWithNames) ORDER BY id"

${CLICKHOUSE_CLIENT} --query="SELECT 'CSV UTF-16 BE BOM:'"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM file('csv_with_utf16be-bom.csv', CSVWithNames) ORDER BY id"

${CLICKHOUSE_CLIENT} --query="SELECT 'CSV UTF-32 LE BOM:'"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM file('csv_with_utf32le-bom.csv', CSVWithNames) ORDER BY id"

${CLICKHOUSE_CLIENT} --query="SELECT 'CSV UTF-32 BE BOM:'"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM file('csv_with_utf32be-bom.csv', CSVWithNames) ORDER BY id"

# Test TSV format
${CLICKHOUSE_CLIENT} --query="SELECT 'TSV UTF-16 LE BOM:'"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM file('tsv_with_utf16le-bom.tsv', TSVWithNames) ORDER BY id"

${CLICKHOUSE_CLIENT} --query="SELECT 'TSV UTF-16 BE BOM:'"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM file('tsv_with_utf16be-bom.tsv', TSVWithNames) ORDER BY id"

${CLICKHOUSE_CLIENT} --query="SELECT 'TSV UTF-32 LE BOM:'"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM file('tsv_with_utf32le-bom.tsv', TSVWithNames) ORDER BY id"

${CLICKHOUSE_CLIENT} --query="SELECT 'TSV UTF-32 BE BOM:'"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM file('tsv_with_utf32be-bom.tsv', TSVWithNames) ORDER BY id"

# Test JSONEachRow format
${CLICKHOUSE_CLIENT} --query="SELECT 'JSONEachRow UTF-16 LE BOM:'"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM file('json_with_utf16le-bom.jsonl', JSONEachRow) ORDER BY id"

${CLICKHOUSE_CLIENT} --query="SELECT 'JSONEachRow UTF-16 BE BOM:'"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM file('json_with_utf16be-bom.jsonl', JSONEachRow) ORDER BY id"

${CLICKHOUSE_CLIENT} --query="SELECT 'JSONEachRow UTF-32 LE BOM:'"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM file('json_with_utf32le-bom.jsonl', JSONEachRow) ORDER BY id"

${CLICKHOUSE_CLIENT} --query="SELECT 'JSONEachRow UTF-32 BE BOM:'"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM file('json_with_utf32be-bom.jsonl', JSONEachRow) ORDER BY id"

# Test JSONColumns format (columnar JSON: {"id":[1,2],"name":["Alice","Bob"]})
${CLICKHOUSE_CLIENT} --query="SELECT 'JSONColumns UTF-16 LE BOM:'"
${CLICKHOUSE_CLIENT} --query="SELECT id, name FROM file('jsoncolumns_with_utf16le-bom.json', JSONColumns, 'id Int32, name String') ORDER BY id"

${CLICKHOUSE_CLIENT} --query="SELECT 'JSONColumns UTF-16 BE BOM:'"
${CLICKHOUSE_CLIENT} --query="SELECT id, name FROM file('jsoncolumns_with_utf16be-bom.json', JSONColumns, 'id Int32, name String') ORDER BY id"

# Test JSONCompactColumns format (columnar JSON: [[1,2],["Alice","Bob"]])
${CLICKHOUSE_CLIENT} --query="SELECT 'JSONCompactColumns UTF-16 LE BOM:'"
${CLICKHOUSE_CLIENT} --query="SELECT id, name FROM file('jsoncompactcolumns_with_utf16le-bom.json', JSONCompactColumns, 'id Int32, name String') ORDER BY id"

${CLICKHOUSE_CLIENT} --query="SELECT 'JSONCompactColumns UTF-16 BE BOM:'"
${CLICKHOUSE_CLIENT} --query="SELECT id, name FROM file('jsoncompactcolumns_with_utf16be-bom.json', JSONCompactColumns, 'id Int32, name String') ORDER BY id"

# Cleanup
rm $USER_FILES_PATH/csv_with_utf8-bom.csv
rm $USER_FILES_PATH/csv_with_utf16le-bom.csv
rm $USER_FILES_PATH/csv_with_utf16be-bom.csv
rm $USER_FILES_PATH/csv_with_utf32le-bom.csv
rm $USER_FILES_PATH/csv_with_utf32be-bom.csv
rm $USER_FILES_PATH/tsv_with_utf16le-bom.tsv
rm $USER_FILES_PATH/tsv_with_utf16be-bom.tsv
rm $USER_FILES_PATH/tsv_with_utf32le-bom.tsv
rm $USER_FILES_PATH/tsv_with_utf32be-bom.tsv
rm $USER_FILES_PATH/json_with_utf16le-bom.jsonl
rm $USER_FILES_PATH/json_with_utf16be-bom.jsonl
rm $USER_FILES_PATH/json_with_utf32le-bom.jsonl
rm $USER_FILES_PATH/json_with_utf32be-bom.jsonl
rm $USER_FILES_PATH/jsoncolumns_with_utf16le-bom.json
rm $USER_FILES_PATH/jsoncolumns_with_utf16be-bom.json
rm $USER_FILES_PATH/jsoncompactcolumns_with_utf16le-bom.json
rm $USER_FILES_PATH/jsoncompactcolumns_with_utf16be-bom.json
