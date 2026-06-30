#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CONFIG_FILE=$CUR_DIR/$(basename "${BASH_SOURCE[0]}" .sh).config.xml
UDF_FILE=$CUR_DIR/../../integration/test_executable_user_defined_function/functions/test_function_config.xml

BASE="$CUR_DIR/03742_functions_deny_list"

mkdir -p "$BASE"/access
cp $CONFIG_FILE "$BASE"/config.xml
cp $UDF_FILE "$BASE"/test_function_config.xml

export CLICKHOUSE_CURL_TIMEOUT=2

# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_WATCHDOG_ENABLE=0 $CLICKHOUSE_SERVER_BINARY --config-file="$BASE"/config.xml &> "$BASE"/clickhouse-server.stderr &
server_pid=$!
port=9742
http_port=3742

CLICKHOUSE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:$http_port"

# wait for the server to start accepting tcp connections (max 30 seconds)
i=0 retries=30
while [[ $i -lt $retries ]] && ! ${CLICKHOUSE_CURL} --max-time 1 -s ${CLICKHOUSE_URL} -d "SELECT 1" 2>&1 >/dev/null; do
    sleep 1
    ((++i))
done
if ! ${CLICKHOUSE_CURL} --max-time 1 -s ${CLICKHOUSE_URL} -d "SELECT 1" 2>&1 >/dev/null; then
    rm -rf "$BASE"
    exit 1
fi

${CLICKHOUSE_CURL} -s ${CLICKHOUSE_URL} -d "CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --port $port --query "SELECT plus(number, 1) FROM system.numbers LIMIT 1"

${CLICKHOUSE_CLIENT} --port $port --query "SELECT * FROM system.functions LIMIT 0" # no exception before

${CLICKHOUSE_CLIENT} --port $port --query "CREATE FUNCTION test_sql_udf AS (value, dictionary) -> if(has(dictionary, value), value, '')"
${CLICKHOUSE_CLIENT} --port $port --query "SELECT test_sql_udf('42', ['some_dict']) FROM system.one LIMIT 0"
${CLICKHOUSE_CLIENT} --port $port --query "SELECT test_function_bash(number) FROM system.numbers LIMIT 1 -- { serverError FUNCTION_NOT_ALLOWED }"
${CLICKHOUSE_CLIENT} --port $port --query "SELECT test_function_pool_bash(number) FROM system.numbers LIMIT 1 -- { serverError FUNCTION_NOT_ALLOWED }"
${CLICKHOUSE_CLIENT} --port $port --query "SELECT test_sql_udf('42', ['some_dict']) FROM system.one LIMIT 0 SETTINGS enable_analyzer=0"
${CLICKHOUSE_CLIENT} --port $port --query "SELECT test_function_bash(number) FROM system.numbers LIMIT 1 SETTINGS enable_analyzer=0 -- { serverError FUNCTION_NOT_ALLOWED }"
${CLICKHOUSE_CLIENT} --port $port --query "SELECT test_function_pool_bash(number) FROM system.numbers LIMIT 1 SETTINGS enable_analyzer=0 -- { serverError FUNCTION_NOT_ALLOWED }"

${CLICKHOUSE_CLIENT} --port $port --query "SELECT * FROM remote('127.0.0.{1,2}', system.numbers) LIMIT 1 -- { serverError FUNCTION_NOT_ALLOWED }"
${CLICKHOUSE_CLIENT} --port $port --query "SELECT * FROM remoteSecure('127.0.0.{1,2}', system.numbers) LIMIT 1 -- { serverError FUNCTION_NOT_ALLOWED }"
${CLICKHOUSE_CLIENT} --port $port --query "SELECT * FROM remote('127.0.0.{1,2}', system.numbers) LIMIT 1 SETTINGS enable_analyzer=0 -- { serverError FUNCTION_NOT_ALLOWED }"
${CLICKHOUSE_CLIENT} --port $port --query "SELECT * FROM remoteSecure('127.0.0.{1,2}', system.numbers) LIMIT 1 SETTINGS enable_analyzer=0 -- { serverError FUNCTION_NOT_ALLOWED }"
${CLICKHOUSE_CLIENT} --port $port --query "SELECT * FROM REMOTE('127.0.0.{1,2}', system.numbers) LIMIT 1 -- { serverError UNKNOWN_FUNCTION }"
${CLICKHOUSE_CLIENT} --port $port --query "SELECT * FROM REMOTESECURE('127.0.0.{1,2}', system.numbers) LIMIT 1 -- { serverError UNKNOWN_FUNCTION }"

${CLICKHOUSE_CLIENT} --port $port --query "SELECT * FROM VALUES('person String, place String', ('Noah', 'Paris')) -- { serverError FUNCTION_NOT_ALLOWED }"
${CLICKHOUSE_CLIENT} --port $port --query "SELECT * FROM values('person String, place String', ('Noah', 'Paris')) -- { serverError FUNCTION_NOT_ALLOWED }"
${CLICKHOUSE_CLIENT} --port $port --query "SELECT * FROM FORMAT(CSV, '0, 1') -- { serverError FUNCTION_NOT_ALLOWED }"
${CLICKHOUSE_CLIENT} --port $port --query "SELECT * FROM format(CSV, '0, 1') -- { serverError FUNCTION_NOT_ALLOWED }"

${CLICKHOUSE_CURL} -sS -i "${CLICKHOUSE_URL}" -d "SELECT * FROM remote('127.0.0.{1,2}', system.numbers) LIMIT 1" | grep -o -F 'FUNCTION_NOT_ALLOWED'
${CLICKHOUSE_CURL} -sS -i "${CLICKHOUSE_URL}" -d "SELECT * FROM REMOTE('127.0.0.{1,2}', system.numbers) LIMIT 1" | grep -o -F 'UNKNOWN_FUNCTION'

${CLICKHOUSE_CLIENT} --port $port --query "SELECT * FROM system.functions LIMIT 0" # no exception after

kill $server_pid
wait $server_pid
return_code=$?

rm -rf "$BASE"
exit $return_code
