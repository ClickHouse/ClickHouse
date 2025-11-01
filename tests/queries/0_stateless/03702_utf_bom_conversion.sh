#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh


# UTF-8 BOM (EF BB BF)
printf '\xEF\xBB\xBF' > user_files/test_utf8_bom_${CLICKHOUSE_TEST_UNIQUE_NAME}.csv
printf 'id,name\n1,Alice\n2,Bob\n' >> user_files/test_utf8_bom_${CLICKHOUSE_TEST_UNIQUE_NAME}.csv

# UTF-16 LE BOM (FF FE)
printf '\xFF\xFE' > user_files/test_utf16le_bom_${CLICKHOUSE_TEST_UNIQUE_NAME}.csv
# Add UTF-16 LE encoded data
printf 'i\x00d\x00,\x00n\x00a\x00m\x00e\x00\n\x00' >> user_files/test_utf16le_bom_${CLICKHOUSE_TEST_UNIQUE_NAME}.csv
printf '1\x00,\x00A\x00l\x00i\x00c\x00e\x00\n\x00' >> user_files/test_utf16le_bom_${CLICKHOUSE_TEST_UNIQUE_NAME}.csv

# UTF-16 BE BOM (FE FF)
printf '\xFE\xFF' > user_files/test_utf16be_bom_${CLICKHOUSE_TEST_UNIQUE_NAME}.csv
printf '\x00i\x00d\x00,\x00n\x00a\x00m\x00e\x00\n' >> user_files/test_utf16be_bom_${CLICKHOUSE_TEST_UNIQUE_NAME}.csv
printf '\x001\x00,\x00A\x00l\x00i\x00c\x00e\x00\n' >> user_files/test_utf16be_bom_${CLICKHOUSE_TEST_UNIQUE_NAME}.csv

${CLICKHOUSE_CLIENT} --query="SELECT 'UTF-8 BOM Test:'"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM file('test_utf8_bom_${CLICKHOUSE_TEST_UNIQUE_NAME}.csv', CSVWithNames) ORDER BY id"

${CLICKHOUSE_CLIENT} --query="SELECT 'UTF-16 LE BOM Test:'"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM file('test_utf16le_bom_${CLICKHOUSE_TEST_UNIQUE_NAME}.csv', CSVWithNames) ORDER BY id"

${CLICKHOUSE_CLIENT} --query="SELECT 'UTF-16 BE BOM Test:'"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM file('test_utf16be_bom_${CLICKHOUSE_TEST_UNIQUE_NAME}.csv', CSVWithNames) ORDER BY id"

# Cleanup
rm -f user_files/test_*_${CLICKHOUSE_TEST_UNIQUE_NAME}.csv
