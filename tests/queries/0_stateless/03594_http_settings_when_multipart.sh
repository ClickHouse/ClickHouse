#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


CLICKHOUSE_URL="${CLICKHOUSE_URL}&session_timezone=Asia/Novosibirsk"
CLICKHOUSE_URL="${CLICKHOUSE_URL}&input_format_binary_read_json_as_string=1"
CLICKHOUSE_URL="${CLICKHOUSE_URL}&date_time_input_format=best_effort"


$CLICKHOUSE_CURL_COMMAND -sS $CLICKHOUSE_URL --data-binary @- <<EOF
    CREATE OR REPLACE TABLE 03694_table (
        name String,
        doc JSON (
            createTime DateTime('Asia/Novosibirsk'),
            id String))
    ENGINE = MergeTree
    ORDER BY (name);
EOF

(
    echo -ne '--multipart-from-data-boundary'
    echo -ne '\r\n'
    echo -ne 'Content-Type: text/plain; charset=utf-8'
    echo -ne '\r\n'
    echo -ne 'Content-Disposition: form-data; name=query'
    echo -ne '\r\n'
    echo -ne '\r\n'
    echo -ne 'INSERT INTO 03694_table values'
    echo -ne "('first_row', '{\"createTime\":\"2024-07-09T14:06:05.083Z\", \"id\": \"id_first_row\"}')"
    echo -ne '\r\n'
    echo -ne '--multipart-from-data-boundary--'
    echo -ne '\r\n'
) | \
$CLICKHOUSE_CURL_COMMAND -sS \
    $CLICKHOUSE_URL \
    -H "Accept: application/json, text/csv, application/octet-stream" \
    -H "Accept-Encoding: gzip, deflate" \
    -H "Content-Type: multipart/form-data; boundary=multipart-from-data-boundary" \
    --data-binary @-

$CLICKHOUSE_CURL_COMMAND -sS \
    $CLICKHOUSE_URL \
    --data-binary @- <<EOF
    INSERT INTO 03694_table VALUES ('second_row', '{"createTime":"2024-07-09T14:06:05.083Z", "id": "id_second_row"}')
EOF

$CLICKHOUSE_CURL_COMMAND -sS \
    $CLICKHOUSE_URL \
    --data-binary @- <<EOF
SELECT * FROM 03694_table ORDER BY name FORMAT JSONEachRow
EOF
