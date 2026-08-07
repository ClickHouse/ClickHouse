#!/usr/bin/env bash

# NOTE: this sh wrapper is required because of shell_config

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists test_tbl"
$CLICKHOUSE_CLIENT -q "create table test_tbl (d Nullable(Int64)) engine=Memory"
$CLICKHOUSE_CLIENT -q "insert into test_tbl select * from numbers(5)"
$CLICKHOUSE_CLIENT -q "select multiIf(d > 0, 1, -1), multiIf(d > 1, d-1, -1), multiIf(d > 2, null, -1) from test_tbl"
$CLICKHOUSE_CLIENT -q "drop table test_tbl"