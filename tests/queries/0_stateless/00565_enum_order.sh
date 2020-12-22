#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

set -e -o pipefail

$CLICKHOUSE_CLIENT <<"EOF"
DROP TABLE IF EXISTS `test_log`
EOF

$CLICKHOUSE_CLIENT <<"EOF"
CREATE TABLE `test_log` (
    date Date,
    datetime DateTime,
    path String,
    gtid String,
    query_serial UInt32,
    row_serial UInt32,
    reqid Int64,
    method String,
    service String,
    db String,
    type String,
    operation Enum8('INSERT'=1, 'UPDATE'=2, 'DELETE'=3),
    old_fields Nested(name String, value String, is_null Enum8('true'=1, 'false'=0)),
    new_fields Nested(name String, value String, is_null Enum8('true'=1, 'false'=0)),
    record_source_type Int8,
    record_source_timestamp DateTime,
    deleted Enum8('true'=1, 'false'=0)
) ENGINE = MergeTree(
    date,
    (date, path, gtid, query_serial, row_serial),
    1024
)
EOF

DATA='2018-01-01\t2018-01-01 03:00:00\tclient:1-\tserveruuid:0\t0\t0\t0\t\t\ttest\ttest\tINSERT\t[]\t[]\t[]\t[]\t[]\t[]\t1\t2018-02-02 15:54:10\tfalse\n'
QUERY='INSERT INTO "test_log"("date", "datetime", "path", "gtid", "query_serial", "row_serial",
    "reqid", "method", "service", "db", "type", "operation", "old_fields"."name",
    "old_fields"."value", "old_fields"."is_null", "new_fields"."name", "new_fields"."value",
    "new_fields"."is_null", "record_source_type", "record_source_timestamp", "deleted") FORMAT TabSeparated'
QUERY="$(tr -d '\n' <<<"$QUERY")"
echo "$QUERY"
URL=$(python3 -c 'import urllib.parse; print("'"${CLICKHOUSE_URL}"'&query=" + urllib.parse.quote('"'''$QUERY'''"'))')

set +e
for _ in 1 2 3; do
    echo run by native protocol
    echo -ne "$DATA" | $CLICKHOUSE_CLIENT --query "$QUERY"

    echo run by http protocol
    echo -ne "$DATA" | $CLICKHOUSE_CURL -sS -X POST --data-binary @- "$URL"
done

echo 'Count:'
$CLICKHOUSE_CLIENT --query 'select count() from test_log'
$CLICKHOUSE_CLIENT --query 'DROP TABLE test_log'
