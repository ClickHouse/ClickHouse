#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


test_name=$(basename "${BASH_SOURCE[0]}")
tmp_file=${CLICKHOUSE_TMP}/${test_name}_data.csv

cat >$tmp_file <<EOF
1,John,30
2,Jane,25
3,Bob,40
EOF

echo "HTTP POST with external table"
${CLICKHOUSE_CURL} -sS -X POST "${CLICKHOUSE_URL}?query=SELECT%20*%20FROM%20external_table" \
        -F "external_table_structure=id UInt32, name String, age UInt8" \
        -F "external_table_format=CSV" \
        -F "external_table=@$tmp_file" \
        -F "external_table_structure=id UInt32, name String, age UInt8" \
        -F "external_table_format=CSV" \
        -F "external_table=@$tmp_file" | grep TABLE_ALREADY_EXISTS | sed 's/DB::Exception:/.../'

echo "TCP POST with external table"
${CLICKHOUSE_CLIENT} \
        --query "SELECT * FROM external_table" \
        --external \
        --file=$tmp_file \
        --name=external_table \
        --structure="id UInt32, name String, age UInt8" \
        --format=CSV \
        --external \
        --file=$tmp_file \
        --name=external_table \
        --structure="id UInt32, name String, age UInt8" \
        --format=CSV
