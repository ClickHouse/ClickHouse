#!/usr/bin/env bash
# Tags: no-replicated-database, no-async-insert

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

on_exit() {
${CLICKHOUSE_CLIENT} -m --query "
    SYSTEM DISABLE FAILPOINT output_format_sleep_on_progress;
"
}

trap on_exit EXIT

${CLICKHOUSE_CLIENT} -m --query "SYSTEM ENABLE FAILPOINT output_format_sleep_on_progress;"

query="
SELECT number
FROM (SELECT number FROM system.numbers_mt LIMIT 10)
LIMIT 5
"


SETTINGS="default_format=JSONEachRowWithProgress"
SETTINGS="${SETTINGS}&interactive_delay=1"
SETTINGS="${SETTINGS}&max_block_size=1&max_threads=10"

${CLICKHOUSE_CURL} -sS -v "${CLICKHOUSE_URL}&${SETTINGS}" -d "$query"
