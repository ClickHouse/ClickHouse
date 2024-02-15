#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# This should fail
${CLICKHOUSE_CURL} -X GET -sS "${CLICKHOUSE_URL}&session_id=${SESSION_ID}&query=CREATE+DATABASE+non_post_request_test" | grep -o "Cannot execute query in readonly mode"

# This should fail
${CLICKHOUSE_CURL} --head -sS "${CLICKHOUSE_URL}&session_id=${SESSION_ID}&query=CREATE+DATABASE+non_post_request_test" | grep -o "Internal Server Error"

# This should pass - but will throw error "non_post_request_test already exists" if the database was created by any of the above requests.
${CLICKHOUSE_CURL} -X POST -sS "${CLICKHOUSE_URL}&session_id=${SESSION_ID}" -d 'CREATE DATABASE non_post_request_test'
${CLICKHOUSE_CURL} -X POST -sS "${CLICKHOUSE_URL}&session_id=${SESSION_ID}" -d 'DROP DATABASE non_post_request_test'
