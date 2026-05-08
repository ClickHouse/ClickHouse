#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CONFIG_FILE=$CURDIR/$(basename "${BASH_SOURCE[0]}" .sh).config.xml
UDF_FILE=$CURDIR/../../integration/test_executable_user_defined_function/functions/test_function_config.xml

mkdir -p $CURDIR/access_03742
cp $CONFIG_FILE $CURDIR/config.xml
cp $UDF_FILE $CURDIR/test_function_config.xml

export CLICKHOUSE_CURL_TIMEOUT=2

# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_WATCHDOG_ENABLE=0 $CLICKHOUSE_SERVER_BINARY --config-file=$CURDIR/config.xml &> $CURDIR/clickhouse-server.stderr &
server_pid=$!

CLICKHOUSE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:3742"

# wait for the server to start accepting tcp connections (max 30 seconds)
i=0 retries=30
while [[ $i -lt $retries ]] && ! ${CLICKHOUSE_CURL} --max-time 1 -s ${CLICKHOUSE_URL} -d "SELECT 1" 2>&1 >/dev/null; do
    sleep 1
    ((++i))
done
if ! ${CLICKHOUSE_CURL} --max-time 1 -s ${CLICKHOUSE_URL} -d "SELECT 1" 2>&1 >/dev/null; then
    exit 1
fi

${CLICKHOUSE_CURL} -s ${CLICKHOUSE_URL} -d "CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --port 9742 --query "SELECT plus(number, 1) FROM system.numbers LIMIT 1"

${CLICKHOUSE_CLIENT} --port 9742 --query "CREATE FUNCTION IF NOT EXISTS test_sql_udf AS (value, dictionary) -> if(has(dictionary, value), value, '')"
${CLICKHOUSE_CLIENT} --port 9742 --query "SELECT test_sql_udf('42', ['some_dict']) FROM system.one -- { serverError FUNCTION_NOT_ALLOWED }"
${CLICKHOUSE_CLIENT} --port 9742 --query "SELECT test_function_bash(number) FROM system.numbers LIMIT 1 -- { serverError FUNCTION_NOT_ALLOWED }"

${CLICKHOUSE_CLIENT} --port 9742 --query "SELECT * FROM remote('127.0.0.{1,2} LIMIT 1', system.numbers) -- { serverError FUNCTION_NOT_ALLOWED }"
${CLICKHOUSE_CLIENT} --port 9742 --query "SELECT * FROM remoteSecure('127.0.0.{1,2} LIMIT 1', system.numbers) -- { serverError FUNCTION_NOT_ALLOWED }"
${CLICKHOUSE_CLIENT} --port 9742 --query "SELECT * FROM REMOTE('127.0.0.{1,2} LIMIT 1', system.numbers) -- { serverError UNKNOWN_FUNCTION }"
${CLICKHOUSE_CLIENT} --port 9742 --query "SELECT * FROM REMOTESECURE('127.0.0.{1,2} LIMIT 1', system.numbers) -- { serverError UNKNOWN_FUNCTION }"

${CLICKHOUSE_CURL} -sS -i "${CLICKHOUSE_URL}" -d "SELECT * FROM remote('127.0.0.{1,2} LIMIT 1" | grep -o -F 'FUNCTION_NOT_ALLOWED'
${CLICKHOUSE_CURL} -sS -i "${CLICKHOUSE_URL}" -d "SELECT * FROM REMOTE('127.0.0.{1,2} LIMIT 1" | grep -o -F 'UNKNOWN_FUNCTION'

kill $server_pid
wait $server_pid
return_code=$?

exit $return_code
