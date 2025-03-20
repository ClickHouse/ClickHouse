#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "create table dst (number UInt64) engine = MergeTree ORDER BY number;"
${CLICKHOUSE_CLIENT} --query "select number + sleep(0.1) as number from system.numbers limit 10 settings max_block_size = 1 format Native" 2>/dev/null | ${CLICKHOUSE_CLIENT} --max_execution_time 0.3 --timeout_overflow_mode break --query "insert into dst format Native"
