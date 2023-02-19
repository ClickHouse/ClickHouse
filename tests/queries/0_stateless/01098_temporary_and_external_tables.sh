#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

url_without_session="https://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTPS}/?"
url="${url_without_session}session_id=test_01098"

${CLICKHOUSE_CURL} -m 30 -sSk "$url" --data "DROP TEMPORARY TABLE IF EXISTS tmp_table"
${CLICKHOUSE_CURL} -m 30 -sSk "$url" --data "CREATE TEMPORARY TABLE tmp_table AS SELECT number AS n FROM numbers(42)"

# NOTE: database = $CLICKHOUSE_DATABASE is unwanted
id=$(echo "SELECT uuid FROM system.tables WHERE name='tmp_table' AND is_temporary" | ${CLICKHOUSE_CURL} -m 31 -sSgk "$url" -d @-)
internal_table_name="_temporary_and_external_tables.\`_tmp_$id\`"

${CLICKHOUSE_CURL} -m 30 -sSk "$url" --data "SHOW CREATE TEMPORARY TABLE tmp_table"
${CLICKHOUSE_CURL} -m 30 -sSk "$url" --data "SHOW CREATE TABLE $internal_table_name"
${CLICKHOUSE_CURL} -m 30 -sSk "$url_without_session" --data "SHOW CREATE TABLE $internal_table_name"

echo "SELECT COUNT() FROM tmp_table" | ${CLICKHOUSE_CURL} -m 60 -sSgk "$url" -d @-
echo "SELECT COUNT() FROM $internal_table_name" | ${CLICKHOUSE_CURL} -m 60 -sSgk "$url" -d @- | grep -F "Code: 291" > /dev/null && echo "OK"
echo "SELECT COUNT() FROM $internal_table_name" | ${CLICKHOUSE_CURL} -m 60 -sSgk "$url_without_session" -d @- | grep -F "Code: 291" > /dev/null && echo "OK"

echo -ne '0\n1\n' | ${CLICKHOUSE_CURL} -m 30 -sSkF 'file=@-' "$url&file_format=CSV&file_types=UInt64&query=SELECT+sum((number+GLOBAL+IN+(SELECT+number+AS+n+FROM+remote('127.0.0.2',+numbers(5))+WHERE+n+GLOBAL+IN+(SELECT+*+FROM+tmp_table)+AND+n+GLOBAL+NOT+IN+(SELECT+*+FROM+file)+))+AS+res),+sum(number*res)+FROM+remote('127.0.0.2',+numbers(10))"

echo -ne '0\n1\n' | ${CLICKHOUSE_CURL} -m 30 -sSkF 'file=@-' "$url&file_format=CSV&file_types=UInt64&query=SELECT+_1%2BsleepEachRow(3)+FROM+file" &

wait
${CLICKHOUSE_CURL} -m 30 -sSk "$url" --data "DROP TEMPORARY TABLE tmp_table"
