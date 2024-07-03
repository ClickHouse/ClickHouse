#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists test"
$CLICKHOUSE_CLIENT -q "create table test(id UInt64, t DateTime64) Engine=MergeTree order by id"
$CLICKHOUSE_CLIENT -q "insert into test from infile '"$CURDIR"/data_orc/read_time_zone.snappy.orc' SETTINGS input_format_orc_read_use_writer_time_zone=true FORMAT ORC"
$CLICKHOUSE_CLIENT -q "select * from test"
$CLICKHOUSE_CLIENT -q "drop table test"