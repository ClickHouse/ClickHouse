#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "create table null_t (number UInt64) engine = Null;"
${CLICKHOUSE_CLIENT} --query "select sleep(0.1) from system.numbers settings max_block_size = 1 format Native" 2>/dev/null | ${CLICKHOUSE_CLIENT} --max_execution_time 0.3 --timeout_overflow_mode break --query "insert into null_t format Native" 2>&1 | grep -o "QUERY_WAS_CANCELLED"
