#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists test_orc_read_timezone"
$CLICKHOUSE_CLIENT -q "create table test_orc_read_timezone(id UInt64, t DateTime64) Engine=MergeTree order by id"
$CLICKHOUSE_CLIENT -q "insert into test_orc_read_timezone from infile '$CURDIR/data_orc/test_reader_time_zone.snappy.orc' SETTINGS input_format_orc_reader_time_zone_name='Asia/Shanghai' FORMAT ORC"
$CLICKHOUSE_CLIENT -q "select * from test_orc_read_timezone SETTINGS session_timezone='Asia/Shanghai'"
$CLICKHOUSE_CLIENT -q "drop table test_orc_read_timezone"