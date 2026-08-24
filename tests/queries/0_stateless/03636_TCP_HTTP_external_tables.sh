#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


test_name=$(basename "${BASH_SOURCE[0]}")

tmp_file_1=${CLICKHOUSE_TMP}/${test_name}_data_1.csv
cat >$tmp_file_1 <<EOF
1,John,30
2,Jane,25
3,Bob,40
EOF

tmp_file_2=${CLICKHOUSE_TMP}/${test_name}_data_2.csv
cat >$tmp_file_2 <<EOF
4,Alice,28
5,Tom,35
EOF

echo "HTTP POST with external table"
${CLICKHOUSE_CURL} -sS -X POST "${CLICKHOUSE_URL}&query=SELECT%20*%20FROM%20external_table%20order%20by%20id" \
        -F "external_table_structure=id UInt32, name String, age UInt8" \
        -F "external_table_format=CSV" \
        -F "external_table=@$tmp_file_1" \
        -F "external_table_structure=id UInt32, name String, age UInt8" \
        -F "external_table_format=CSV" \
        -F "external_table=@$tmp_file_2"

echo "TCP with external table"
${CLICKHOUSE_CLIENT} \
        --query "SELECT * FROM external_table order by id" \
        --external \
        --file=$tmp_file_1 \
        --name=external_table \
        --structure="id UInt32, name String, age UInt8" \
        --format=CSV \
        --external \
        --file=$tmp_file_2 \
        --name=external_table \
        --structure="id UInt32, name String, age UInt8" \
        --format=CSV
