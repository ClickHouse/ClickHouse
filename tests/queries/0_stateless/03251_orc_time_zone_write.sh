#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists test_orc_timezone"
$CLICKHOUSE_CLIENT -q "create table test_orc_timezone(id UInt64, t DateTime64) ENGINE = File(ORC) SETTINGS output_format_orc_writer_time_zone_name='Asia/Shanghai'"
$CLICKHOUSE_CLIENT -q "insert into test_orc_timezone select number, toDateTime64('2024-06-30 20:00:00', 3, 'Asia/Shanghai') from numbers(5)"
$CLICKHOUSE_CLIENT -q "select * from test_orc_timezone SETTINGS session_timezone='Asia/Shanghai'"
$CLICKHOUSE_CLIENT -q "drop table test_orc_timezone"