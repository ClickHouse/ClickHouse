#!/usr/bin/env bash
# Tags: no-fasttest
# NOTE: this sh wrapper is required because of shell_config

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists test_tbl"
$CLICKHOUSE_CLIENT -q "create table test_tbl (a Int64, b UInt64) Engine=Memory"
$CLICKHOUSE_CLIENT -q "insert into test_tbl values(10262736196, 10262736196)"
$CLICKHOUSE_CLIENT -q "select fromUnixTimestampInJodaSyntax(a, 'YYYY-MM-dd HH:mm:ss', 'Asia/Shanghai'), fromUnixTimestampInJodaSyntax(b, 'YYYY-MM-dd HH:mm:ss', 'Asia/Shanghai') from test_tbl"
$CLICKHOUSE_CLIENT -q "drop table test_tbl"