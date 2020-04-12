#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

url="https://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTPS}/?session_id=test_01098"

${CLICKHOUSE_CURL} -m 30 -sSk "$url" --data "CREATE TEMPORARY TABLE tmp_table AS SELECT number AS n FROM numbers(42)" > /dev/null;

name_expr="'\`' || database || '\`.\`' || name || '\`'"
full_tmp_name=`echo "SELECT $name_expr FROM system.tables WHERE database='_temporary_and_external_tables' AND create_table_query LIKE '%tmp_table%'" | ${CLICKHOUSE_CURL} -m 30 -sSgk $url -d @-`

echo "SELECT * FROM $full_tmp_name" | ${CLICKHOUSE_CURL} -m 60 -sSgk $url -d @- | grep -F "Code: 291" > /dev/null && echo "OK"

echo -ne '0\n1\n' | ${CLICKHOUSE_CURL} -m 30 -sSkF 'file=@-' "$url&file_format=CSV&file_types=UInt64&query=SELECT+sum((number+GLOBAL+IN+(SELECT+number+AS+n+FROM+remote('127.0.0.2',+numbers(5))+WHERE+n+GLOBAL+IN+(SELECT+*+FROM+tmp_table)+AND+n+GLOBAL+NOT+IN+(SELECT+*+FROM+file)+))+AS+res),+sum(number*res)+FROM+remote('127.0.0.2',+numbers(10))";

