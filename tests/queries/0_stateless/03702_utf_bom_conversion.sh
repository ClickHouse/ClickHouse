#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

mkdir -p "$CLICKHOUSE_TEST_UNIQUE_NAME"

# Generate test files dynamically
python3 -c "
import os
dest = os.environ['CLICKHOUSE_TEST_UNIQUE_NAME']
csv_content = 'id,name\n1,Alice\n2,Bob'
tsv_content = 'id\tname\n1\tAlice\n2\tBob\n'
json_content = '{\"id\":1,\"name\":\"Alice\"}\n{\"id\":2,\"name\":\"Bob\"}\n'
jsoncol_content = '{\"id\":[1,2],\"name\":[\"Alice\",\"Bob\"]}'
jsoncompact_content = '[[1,2],[\"Alice\",\"Bob\"]]'

fixtures = {
    'csv_with_utf8-bom.csv': (csv_content + '\n').encode('utf-8-sig'),
    'csv_with_utf16le-bom.csv': b'\xff\xfe' + csv_content.encode('utf-16-le'),
    'csv_with_utf16be-bom.csv': b'\xfe\xff' + csv_content.encode('utf-16-be'),
    'csv_with_utf32le-bom.csv': b'\xff\xfe\x00\x00' + (csv_content + '\n').encode('utf-32-le'),
    'csv_with_utf32be-bom.csv': b'\x00\x00\xfe\xff' + (csv_content + '\n').encode('utf-32-be'),
    'tsv_with_utf16le-bom.tsv': b'\xff\xfe' + tsv_content.encode('utf-16-le'),
    'tsv_with_utf16be-bom.tsv': b'\xfe\xff' + tsv_content.encode('utf-16-be'),
    'tsv_with_utf32le-bom.tsv': b'\xff\xfe\x00\x00' + tsv_content.encode('utf-32-le'),
    'tsv_with_utf32be-bom.tsv': b'\x00\x00\xfe\xff' + tsv_content.encode('utf-32-be'),
    'json_with_utf16le-bom.jsonl': b'\xff\xfe' + json_content.encode('utf-16-le'),
    'json_with_utf16be-bom.jsonl': b'\xfe\xff' + json_content.encode('utf-16-be'),
    'json_with_utf32le-bom.jsonl': b'\xff\xfe\x00\x00' + json_content.encode('utf-32-le'),
    'json_with_utf32be-bom.jsonl': b'\x00\x00\xfe\xff' + json_content.encode('utf-32-be'),
    'jsoncolumns_with_utf16le-bom.json': b'\xff\xfe' + jsoncol_content.encode('utf-16-le'),
    'jsoncolumns_with_utf16be-bom.json': b'\xfe\xff' + jsoncol_content.encode('utf-16-be'),
    'jsoncompactcolumns_with_utf16le-bom.json': b'\xff\xfe' + jsoncompact_content.encode('utf-16-le'),
    'jsoncompactcolumns_with_utf16be-bom.json': b'\xfe\xff' + jsoncompact_content.encode('utf-16-be'),
}
for name, data in fixtures.items():
    with open(os.path.join(dest, name), 'wb') as f:
        f.write(data)
"

# Test CSV format
${CLICKHOUSE_LOCAL} --query="SELECT 'CSV UTF-8 BOM:'"
${CLICKHOUSE_LOCAL} --query="SELECT * FROM file('$CLICKHOUSE_TEST_UNIQUE_NAME/csv_with_utf8-bom.csv', CSVWithNames) ORDER BY id"

${CLICKHOUSE_LOCAL} --query="SELECT 'CSV UTF-16 LE BOM:'"
${CLICKHOUSE_LOCAL} --query="SELECT * FROM file('$CLICKHOUSE_TEST_UNIQUE_NAME/csv_with_utf16le-bom.csv', CSVWithNames) ORDER BY id"

${CLICKHOUSE_LOCAL} --query="SELECT 'CSV UTF-16 BE BOM:'"
${CLICKHOUSE_LOCAL} --query="SELECT * FROM file('$CLICKHOUSE_TEST_UNIQUE_NAME/csv_with_utf16be-bom.csv', CSVWithNames) ORDER BY id"

${CLICKHOUSE_LOCAL} --query="SELECT 'CSV UTF-32 LE BOM:'"
${CLICKHOUSE_LOCAL} --query="SELECT * FROM file('$CLICKHOUSE_TEST_UNIQUE_NAME/csv_with_utf32le-bom.csv', CSVWithNames) ORDER BY id"

${CLICKHOUSE_LOCAL} --query="SELECT 'CSV UTF-32 BE BOM:'"
${CLICKHOUSE_LOCAL} --query="SELECT * FROM file('$CLICKHOUSE_TEST_UNIQUE_NAME/csv_with_utf32be-bom.csv', CSVWithNames) ORDER BY id"

# Test TSV format
${CLICKHOUSE_LOCAL} --query="SELECT 'TSV UTF-16 LE BOM:'"
${CLICKHOUSE_LOCAL} --query="SELECT * FROM file('$CLICKHOUSE_TEST_UNIQUE_NAME/tsv_with_utf16le-bom.tsv', TSVWithNames) ORDER BY id"

${CLICKHOUSE_LOCAL} --query="SELECT 'TSV UTF-16 BE BOM:'"
${CLICKHOUSE_LOCAL} --query="SELECT * FROM file('$CLICKHOUSE_TEST_UNIQUE_NAME/tsv_with_utf16be-bom.tsv', TSVWithNames) ORDER BY id"

${CLICKHOUSE_LOCAL} --query="SELECT 'TSV UTF-32 LE BOM:'"
${CLICKHOUSE_LOCAL} --query="SELECT * FROM file('$CLICKHOUSE_TEST_UNIQUE_NAME/tsv_with_utf32le-bom.tsv', TSVWithNames) ORDER BY id"

${CLICKHOUSE_LOCAL} --query="SELECT 'TSV UTF-32 BE BOM:'"
${CLICKHOUSE_LOCAL} --query="SELECT * FROM file('$CLICKHOUSE_TEST_UNIQUE_NAME/tsv_with_utf32be-bom.tsv', TSVWithNames) ORDER BY id"

# Test JSONEachRow format
${CLICKHOUSE_LOCAL} --query="SELECT 'JSONEachRow UTF-16 LE BOM:'"
${CLICKHOUSE_LOCAL} --query="SELECT * FROM file('$CLICKHOUSE_TEST_UNIQUE_NAME/json_with_utf16le-bom.jsonl', JSONEachRow) ORDER BY id"

${CLICKHOUSE_LOCAL} --query="SELECT 'JSONEachRow UTF-16 BE BOM:'"
${CLICKHOUSE_LOCAL} --query="SELECT * FROM file('$CLICKHOUSE_TEST_UNIQUE_NAME/json_with_utf16be-bom.jsonl', JSONEachRow) ORDER BY id"

${CLICKHOUSE_LOCAL} --query="SELECT 'JSONEachRow UTF-32 LE BOM:'"
${CLICKHOUSE_LOCAL} --query="SELECT * FROM file('$CLICKHOUSE_TEST_UNIQUE_NAME/json_with_utf32le-bom.jsonl', JSONEachRow) ORDER BY id"

${CLICKHOUSE_LOCAL} --query="SELECT 'JSONEachRow UTF-32 BE BOM:'"
${CLICKHOUSE_LOCAL} --query="SELECT * FROM file('$CLICKHOUSE_TEST_UNIQUE_NAME/json_with_utf32be-bom.jsonl', JSONEachRow) ORDER BY id"

# Test JSONColumns format (columnar JSON: {"id":[1,2],"name":["Alice","Bob"]})
${CLICKHOUSE_LOCAL} --query="SELECT 'JSONColumns UTF-16 LE BOM:'"
${CLICKHOUSE_LOCAL} --query="SELECT id, name FROM file('$CLICKHOUSE_TEST_UNIQUE_NAME/jsoncolumns_with_utf16le-bom.json', JSONColumns, 'id Int32, name String') ORDER BY id"

${CLICKHOUSE_LOCAL} --query="SELECT 'JSONColumns UTF-16 BE BOM:'"
${CLICKHOUSE_LOCAL} --query="SELECT id, name FROM file('$CLICKHOUSE_TEST_UNIQUE_NAME/jsoncolumns_with_utf16be-bom.json', JSONColumns, 'id Int32, name String') ORDER BY id"

# Test JSONCompactColumns format (columnar JSON: [[1,2],["Alice","Bob"]])
${CLICKHOUSE_LOCAL} --query="SELECT 'JSONCompactColumns UTF-16 LE BOM:'"
${CLICKHOUSE_LOCAL} --query="SELECT id, name FROM file('$CLICKHOUSE_TEST_UNIQUE_NAME/jsoncompactcolumns_with_utf16le-bom.json', JSONCompactColumns, 'id Int32, name String') ORDER BY id"

${CLICKHOUSE_LOCAL} --query="SELECT 'JSONCompactColumns UTF-16 BE BOM:'"
${CLICKHOUSE_LOCAL} --query="SELECT id, name FROM file('$CLICKHOUSE_TEST_UNIQUE_NAME/jsoncompactcolumns_with_utf16be-bom.json', JSONCompactColumns, 'id Int32, name String') ORDER BY id"

# Cleanup
rm -rf "$CLICKHOUSE_TEST_UNIQUE_NAME"
