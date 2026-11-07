#!/usr/bin/env bash
# Tags: long, no-parallel
# shellcheck disable=SC2015

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


echo "Using non-existent session with the 'session_check' flag will throw exception:"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=nonexistent&session_check=1" --data-binary "SELECT 1" | grep -c -F 'SESSION_NOT_FOUND'

echo "Using non-existent session without the 'session_check' flag will create a new session:"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${CLICKHOUSE_DATABASE}_1" --data-binary "SELECT 1"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${CLICKHOUSE_DATABASE}_1&session_check=0" --data-binary "SELECT 1"

echo "The 'session_timeout' parameter is checked for validity and for the maximum value:"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${CLICKHOUSE_DATABASE}_2&session_timeout=string" --data-binary "SELECT 1" | grep -c -F 'Invalid session timeout'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${CLICKHOUSE_DATABASE}_2&session_timeout=3601" --data-binary "SELECT 1" | grep -c -F 'Maximum session timeout'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${CLICKHOUSE_DATABASE}_2&session_timeout=-1" --data-binary "SELECT 1" | grep -c -F 'Invalid session timeout'

echo "Valid cases are accepted:"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${CLICKHOUSE_DATABASE}_3&session_timeout=0" --data-binary "SELECT 1"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${CLICKHOUSE_DATABASE}_4&session_timeout=3600" --data-binary "SELECT 1"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${CLICKHOUSE_DATABASE}_5&session_timeout=60" --data-binary "SELECT 1"

echo "Sessions are local per user:"
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS test_00463; CREATE USER test_00463; GRANT ALL ON *.* TO test_00463;"

${CLICKHOUSE_CURL} -sS -X POST "${CLICKHOUSE_URL}&session_id=${CLICKHOUSE_DATABASE}_6&session_timeout=600" --data-binary "CREATE TEMPORARY TABLE t (s String)"
${CLICKHOUSE_CURL} -sS -X POST "${CLICKHOUSE_URL}&session_id=${CLICKHOUSE_DATABASE}_6" --data-binary "INSERT INTO t VALUES ('Hello')"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=test_00463&session_id=${CLICKHOUSE_DATABASE}_6&session_check=1" --data-binary "SELECT 1" | grep -c -F 'SESSION_NOT_FOUND'
${CLICKHOUSE_CURL} -sS -X POST "${CLICKHOUSE_URL}&user=test_00463&session_id=${CLICKHOUSE_DATABASE}_6&session_timeout=600" --data-binary "CREATE TEMPORARY TABLE t (s String)"
${CLICKHOUSE_CURL} -sS -X POST "${CLICKHOUSE_URL}&user=test_00463&session_id=${CLICKHOUSE_DATABASE}_6" --data-binary "INSERT INTO t VALUES ('World')"

${CLICKHOUSE_CURL} -sS -X POST "${CLICKHOUSE_URL}&session_id=${CLICKHOUSE_DATABASE}_6" --data-binary "SELECT * FROM t"
${CLICKHOUSE_CURL} -sS -X POST "${CLICKHOUSE_URL}&user=test_00463&session_id=${CLICKHOUSE_DATABASE}_6" --data-binary "SELECT * FROM t"

${CLICKHOUSE_CLIENT} --query "DROP USER test_00463";

echo "And cannot be accessed for a non-existent user:"
${CLICKHOUSE_CURL} -sS -X POST "${CLICKHOUSE_URL}&user=test_00463&session_id=${CLICKHOUSE_DATABASE}_6" --data-binary "SELECT * FROM t" | grep -c -F 'Exception'

echo "The temporary tables created in a session are not accessible without entering this session:"
${CLICKHOUSE_CURL} -sS -X POST "${CLICKHOUSE_URL}" --data-binary "SELECT * FROM t" | grep -c -F 'Exception'

echo "A session successfully expire after a timeout:"
# An infinite loop is required to make the test reliable. We will check that the timeout corresponds to the observed time at least once
while true
do
    (
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${CLICKHOUSE_DATABASE}_7&session_timeout=1" --data-binary "SELECT 1"
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${CLICKHOUSE_DATABASE}_7&session_check=1" --data-binary "SELECT 1"
        sleep 3
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${CLICKHOUSE_DATABASE}_7&session_check=1" --data-binary "SELECT 1" | grep -c -F 'SESSION_NOT_FOUND'
    ) | tr -d '\n' | grep -F '111' && break || sleep 1
done

echo "A session successfully expire after a timeout and the session's temporary table shadows the permanent table:"
# An infinite loop is required to make the test reliable. We will check that the timeout corresponds to the observed time at least once
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t; CREATE TABLE t (s String) ENGINE = Memory; INSERT INTO t VALUES ('World');"
while true
do
    (
        ${CLICKHOUSE_CURL} -X POST -sS "${CLICKHOUSE_URL}&session_id=${CLICKHOUSE_DATABASE}_8&session_timeout=1" --data-binary "CREATE TEMPORARY TABLE t (s String)"
        ${CLICKHOUSE_CURL} -X POST -sS "${CLICKHOUSE_URL}&session_id=${CLICKHOUSE_DATABASE}_8" --data-binary "INSERT INTO t VALUES ('Hello')"
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${CLICKHOUSE_DATABASE}_8" --data-binary "SELECT * FROM t"
        sleep 3
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${CLICKHOUSE_DATABASE}_8" --data-binary "SELECT * FROM t"
    ) | tr -d '\n' | grep -F 'HelloWorld' && break || sleep 1
done
${CLICKHOUSE_CLIENT} --query "DROP TABLE t"

echo "A session cannot be used by concurrent connections:"

${CLICKHOUSE_CURL} -sS -X POST "${CLICKHOUSE_URL}&session_id=${CLICKHOUSE_DATABASE}_9&query_id=${CLICKHOUSE_DATABASE}_9&max_rows_to_read=0" --data-binary "SELECT count() FROM system.numbers" >/dev/null &

# An infinite loop is required to make the test reliable. We will ensure that at least once the query on the line above has started before this check
while true
do
    ${CLICKHOUSE_CLIENT} --query "SELECT count() > 0 FROM system.processes WHERE query_id = '${CLICKHOUSE_DATABASE}_9'" | grep -F '1' && break || sleep 1
done

${CLICKHOUSE_CURL} -sS -X POST "${CLICKHOUSE_URL}&session_id=${CLICKHOUSE_DATABASE}_9" --data-binary "SELECT 1" | grep -c -F 'SESSION_IS_LOCKED'
${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '${CLICKHOUSE_DATABASE}_9' SYNC FORMAT Null";
wait
