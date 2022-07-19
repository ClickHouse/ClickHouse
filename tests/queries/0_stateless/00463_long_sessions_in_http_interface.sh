#!/usr/bin/env bash
# Tags: long, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

request() {
    local url="$1"
    local select="$2"
    ${CLICKHOUSE_CURL} --silent "$url" --data "$select"
}


create_temporary_table() {
    local url="$1"
    request "$url" "CREATE TEMPORARY TABLE temp (x String)"
    request "$url" "INSERT INTO temp VALUES ('Hello'), ('World')"
}


check() {
    local url="$1"
    local select="$2"
    local output="$3"
    local expected_result="$4"
    local message="$5"
    result=$(request "$url" "$select" | grep --count "$output")
    if [ "$result" -ne "$expected_result" ]; then
        echo "FAILED: $message"
        exit 1
    fi
}


address=${CLICKHOUSE_HOST}
port=${CLICKHOUSE_PORT_HTTP}
url="${CLICKHOUSE_PORT_HTTP_PROTO}://$address:$port/"
session="?session_id=test_$$"  # use PID for session ID
select="SELECT * FROM system.settings WHERE name = 'max_rows_to_read'"
select_from_temporary_table="SELECT * FROM temp ORDER BY x"
select_from_non_existent_table="SELECT * FROM no_such_table ORDER BY x"


check "$url?session_id=no_such_session_$$&session_check=1" "$select" "Exception.*Session not found" 1 "session_check=1 does not work."
check "$url$session&session_check=0" "$select" "Exception" 0 "session_check=0 does not work."

request "$url""$session" "SET max_rows_to_read=7777777"

check "$url$session&session_timeout=string" "$select" "Exception.*Invalid session timeout" 1 "Non-numeric value accepted as a timeout."
check "$url$session&session_timeout=3601" "$select" "Exception.*Maximum session timeout*" 1 "More then 3600 seconds accepted as a timeout."
check "$url$session&session_timeout=-1" "$select" "Exception.*Invalid session timeout" 1 "Negative timeout accepted."
check "$url$session&session_timeout=0" "$select" "Exception" 0 "Zero timeout not accepted."
check "$url$session&session_timeout=3600" "$select" "Exception" 0 "3600 second timeout not accepted."
check "$url$session&session_timeout=60" "$select" "Exception" 0 "60 second timeout not accepted."

check "$url""$session" "$select" "7777777" 1 "Failed to reuse session."
# Workaround here
# TODO: move the test to integration test or add readonly user to test environment
if [[ -z $(request "$url?user=readonly" "SELECT ''") ]]; then
    # We have readonly user
    check "$url$session&user=readonly&session_check=1" "$select" "Exception.*Session not found" 1 "Session is accessable for another user."
else
    check "$url$session&user=readonly&session_check=1" "$select" "Exception.*Unknown user*" 1 "Session is accessable for unknown user."
fi

create_temporary_table "$url""$session"
check "$url""$session" "$select_from_temporary_table" "Hello" 1 "Failed to reuse a temporary table for session."

check "$url?session_id=another_session_$$" "$select_from_temporary_table" "Exception.*Table .* doesn't exist." 1 "Temporary table is visible for another table."


( (
cat <<EOF
POST /$session HTTP/1.1
Host: $address:$port
Accept: */*
Content-Length: 62
Content-Type: application/x-www-form-urlencoded

EOF
sleep 4
) | telnet "$address" "$port" >/dev/null 2>/dev/null) &
sleep 1
check "$url""$session" "$select" "Exception.*Session is locked" 1 "Double access to the same session."


session="?session_id=test_timeout_$$"

create_temporary_table "$url$session&session_timeout=1"
check "$url$session&session_timeout=1" "$select_from_temporary_table" "Hello" 1 "Failed to reuse a temporary table for session."
sleep 3
check "$url$session&session_check=1" "$select" "Exception.*Session not found" 1 "Session did not expire on time."

create_temporary_table "$url$session&session_timeout=2"
for _ in $(seq 1 3); do
    check "$url$session&session_timeout=2" "$select_from_temporary_table" "Hello" 1 "Session expired too early."
    sleep 1
done
sleep 3
check "$url$session&session_check=1" "$select" "Exception.*Session not found" 1 "Session did not expire on time."

create_temporary_table "$url$session&session_timeout=2"
for _ in $(seq 1 5); do
    check "$url$session&session_timeout=2" "$select_from_non_existent_table" "Exception.*Table .* doesn't exist." 1 "Session expired too early."
    sleep 1
done
check "$url$session&session_timeout=2" "$select_from_temporary_table" "Hello" 1 "Session expired too early. Failed to update timeout in case of exceptions."
sleep 4
check "$url$session&session_check=1" "$select" "Exception.*Session not found" 1 "Session did not expire on time."


echo "PASSED"
