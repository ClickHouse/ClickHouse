#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists test"
$CLICKHOUSE_CLIENT -q "create table test (x UInt32) engine=Memory"

$CLICKHOUSE_CLIENT -q "select 42 format TSV" | $CLICKHOUSE_CLIENT -q "insert into test select * from input('x UInt32') format TSV
   "

$CLICKHOUSE_CLIENT -q "select * from test"
$CLICKHOUSE_CLIENT -q "drop table test"

