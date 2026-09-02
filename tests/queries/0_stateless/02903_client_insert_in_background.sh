#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists test"
$CLICKHOUSE_CLIENT -q "create table test (x UInt64) engine=Memory"
nohup $CLICKHOUSE_CLIENT -q "insert into test values (42)" 2> $CLICKHOUSE_TEST_UNIQUE_NAME.out
tail -n +2 $CLICKHOUSE_TEST_UNIQUE_NAME.out
$CLICKHOUSE_CLIENT -q "drop table test"
rm $CLICKHOUSE_TEST_UNIQUE_NAME.out

