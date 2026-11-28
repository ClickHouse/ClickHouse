#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `clickhouse local` sent a progress packet on every poll when `interactive_delay` was shorter than a poll cycle and
# never pulled the result: `SELECT 1 SETTINGS interactive_delay = 0` spun forever at 100% CPU.
# Found by json_ast_sql_execution_fuzzer.
timeout 60 ${CLICKHOUSE_LOCAL} --query "SELECT 1 SETTINGS interactive_delay = 0"
timeout 60 ${CLICKHOUSE_LOCAL} --query "SELECT 2 SETTINGS interactive_delay = 1"
timeout 60 ${CLICKHOUSE_LOCAL} --interactive_delay 0 --query "SELECT sum(number) FROM numbers(100000)"
timeout 60 ${CLICKHOUSE_LOCAL} --interactive_delay 0 --send_logs_level=trace --query "SELECT count() FROM numbers_mt(1000000)" 2>/dev/null
