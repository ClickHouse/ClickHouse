#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

cp "$CURDIR"/data_csv/csv_with_utf8-bom.csv $USER_FILES_PATH/
cp "$CURDIR"/data_csv/csv_with_utf16le-bom.csv $USER_FILES_PATH/
cp "$CURDIR"/data_csv/csv_with_utf16be-bom.csv $USER_FILES_PATH/

${CLICKHOUSE_CLIENT} --query="SELECT 'UTF-8 BOM Test:'"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM file('csv_with_utf8-bom.csv', CSVWithNames) ORDER BY id"

${CLICKHOUSE_CLIENT} --query="SELECT 'UTF-16 LE BOM Test:'"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM file('csv_with_utf16le-bom.csv', CSVWithNames) ORDER BY id"

${CLICKHOUSE_CLIENT} --query="SELECT 'UTF-16 BE BOM Test:'"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM file('csv_with_utf16be-bom.csv', CSVWithNames) ORDER BY id"

rm $USER_FILES_PATH/csv_with_utf8-bom.csv
rm $USER_FILES_PATH/csv_with_utf16le-bom.csv
rm $USER_FILES_PATH/csv_with_utf16be-bom.csv
