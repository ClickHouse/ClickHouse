#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

timeout --foreground --kill-after=30s -s INT 6s $CLICKHOUSE_CLIENT --max_block_size 1 -m -q "
    SELECT sleep(1) FROM numbers(100) FORMAT Null;
    SELECT 'FAIL';
"

timeout --foreground --kill-after=30s -s INT 6s $CLICKHOUSE_LOCAL --max_block_size 1 -m -q "
    SELECT sleep(1) FROM numbers(100) FORMAT Null;
    SELECT 'FAIL';
"

exit 0
