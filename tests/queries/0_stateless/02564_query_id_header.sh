#!/usr/bin/env bash
# Tags: memory-engine

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_TIMEZONE_ESCAPED=$($CLICKHOUSE_CLIENT --query="SELECT timezone()" | sed 's/[]\/$*.^+:()[]/\\&/g')

# Remove randomized session_timezone from URL so that X-ClickHouse-Timezone header matches the server timezone
CLICKHOUSE_URL_WO_SESSION_TZ=$(echo "${CLICKHOUSE_URL}" | sed 's/\&session_timezone\=[A-Za-z0-9\/\%\_\-\+\-]*//g' | sed 's/\?session_timezone\=[A-Za-z0-9\/\%\_\-\+\-]*\&/\?/g')

function run_and_check_headers()
{
    query=$1
    query_id="${CLICKHOUSE_DATABASE}_${RANDOM}"

    echo "$query"

    ${CLICKHOUSE_CURL} -sS -v "${CLICKHOUSE_URL_WO_SESSION_TZ}&query_id=$query_id" -d "$1" 2>&1 \
        | grep -e "< X-ClickHouse-Query-Id" -e "< X-ClickHouse-Timezone" -e "< X-ClickHouse-Format" -e "< Content-Type" \
        | sed "s/$CLICKHOUSE_TIMEZONE_ESCAPED/timezone/" \
        | sed "s/$query_id/query_id/" \
        | sed "s/\r$//" \
        | sort
}

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_query_id_header"

run_and_check_headers "CREATE TABLE t_query_id_header (a UInt64) ENGINE = Memory"
run_and_check_headers "INSERT INTO t_query_id_header VALUES (1)"
run_and_check_headers "EXISTS TABLE t_query_id_header"
run_and_check_headers "SELECT * FROM t_query_id_header"
run_and_check_headers "DROP TABLE t_query_id_header"
run_and_check_headers "BAD SQL"
