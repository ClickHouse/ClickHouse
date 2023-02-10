#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists test"
$CLICKHOUSE_CLIENT -q "create table test (x UInt64, y UInt64 default 42) engine=Memory"

$CLICKHOUSE_CLIENT -q "select number as x from numbers(2) format Native" | $CLICKHOUSE_CLIENT -q "insert into test settings input_format_defaults_for_omitted_fields=0 format Native"
$CLICKHOUSE_CLIENT -q "select * from test"
$CLICKHOUSE_CLIENT -q "truncate table test"

$CLICKHOUSE_CLIENT -q "select number as x from numbers(2) format Native" | $CLICKHOUSE_CLIENT -q "insert into test settings input_format_defaults_for_omitted_fields=1 format Native"
$CLICKHOUSE_CLIENT -q "select * from test"
$CLICKHOUSE_CLIENT -q "truncate table test"
