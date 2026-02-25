#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

yes 'CREATE TABLE IF NOT EXISTS table (x UInt8) ENGINE = MergeTree ORDER BY tuple();' | head -n 1000 | $CLICKHOUSE_CLIENT &
yes 'DROP TABLE IF EXISTS table;' | head -n 1000 | $CLICKHOUSE_CLIENT &
wait

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS table"
