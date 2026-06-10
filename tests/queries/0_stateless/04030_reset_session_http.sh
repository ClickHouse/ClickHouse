#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Exercise `RESET SESSION` over the HTTP interface using `session_id` to carry
# session state across requests. We deliberately avoid asserting against
# settings here — every HTTP request appends settings to the URL params, so
# per-request settings would override session settings regardless of what
# RESET SESSION does. The query-parameter and temp-table semantics are not
# similarly overridden and so are testable.

SID="reset_session_http_${CLICKHOUSE_DATABASE}"

http() {
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${SID}&session_timeout=60" --data-binary "$1"
}

echo "-- pre-reset --"
http "SET param_x = 'mango'"
http "SET param_y = '7'"
http "CREATE TEMPORARY TABLE reset_t (x Int) ENGINE = Memory"
http "INSERT INTO reset_t VALUES (1), (2), (3)"
http "SELECT {x:String}, {y:UInt32}, count() FROM reset_t"

http "RESET SESSION"

echo "-- post-reset --"
# query parameters must be gone.
http "SELECT {x:String}" 2>&1 | grep -c 'UNKNOWN_QUERY_PARAMETER'
http "SELECT {y:UInt32}" 2>&1 | grep -c 'UNKNOWN_QUERY_PARAMETER'
# temp table must be gone.
http "SELECT * FROM reset_t" 2>&1 | grep -c 'UNKNOWN_TABLE'

echo "-- idempotent --"
http "RESET SESSION"
http "RESET SESSION"
http "SELECT 'ok'"

echo "-- session still usable --"
http "SET param_z = 'banana'"
http "CREATE TEMPORARY TABLE reset_t (n Int) ENGINE = Memory"
http "INSERT INTO reset_t VALUES (99)"
http "SELECT {z:String}, * FROM reset_t"
