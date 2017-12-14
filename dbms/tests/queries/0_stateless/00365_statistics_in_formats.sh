#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT --query="SELECT number FROM system.numbers LIMIT 10 FORMAT JSON" | grep 'rows_read';
$CLICKHOUSE_CLIENT --query="SELECT number FROM system.numbers LIMIT 10 FORMAT JSONCompact" | grep 'rows_read';
$CLICKHOUSE_CLIENT --query="SELECT number FROM system.numbers LIMIT 10 FORMAT XML" | grep 'rows_read';

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number FROM system.numbers LIMIT 10 FORMAT JSON" | grep 'rows_read';
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number FROM system.numbers LIMIT 10 FORMAT JSONCompact" | grep 'rows_read';
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number FROM system.numbers LIMIT 10 FORMAT XML" | grep 'rows_read';
