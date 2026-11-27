#!/usr/bin/env bash
# Tags: no-replicated-database, no-async-insert, no-parallel

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
SELECT 1
FROM (SELECT number FROM system.numbers_mt LIMIT 2)
LIMIT 1
"

SETTINGS="default_format=JSONEachRowWithProgress"
SETTINGS="${SETTINGS}&interactive_delay=1"
SETTINGS="${SETTINGS}&max_block_size=1&max_threads=10"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&${SETTINGS}" -d "$query" | sed 's/^{"progress":{.*}$//g' | grep -v "^$"
