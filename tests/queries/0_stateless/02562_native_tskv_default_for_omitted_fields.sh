#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists test"
$CLICKHOUSE_CLIENT -q "insert into function file(02562_data.native) select 1::UInt64 as x settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "create table test (x UInt64, y UInt64 default 42) engine=File(Native, '02562_data.native') settings input_format_defaults_for_omitted_fields=0"
$CLICKHOUSE_CLIENT -q "select * from test"
$CLICKHOUSE_CLIENT -q "drop table test"

$CLICKHOUSE_CLIENT -q "create table test (x UInt64, y UInt64 default 42) engine=File(Native, '02562_data.native') settings input_format_defaults_for_omitted_fields=1"
$CLICKHOUSE_CLIENT -q "select * from test"
$CLICKHOUSE_CLIENT -q "drop table test"

$CLICKHOUSE_CLIENT -q "insert into function file(02562_data.tskv) select 1::UInt64 as x settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "create table test (x UInt64, y UInt64 default 42) engine=File(TSKV, '02562_data.tskv') settings input_format_defaults_for_omitted_fields=0"
$CLICKHOUSE_CLIENT -q "select * from test"
$CLICKHOUSE_CLIENT -q "drop table test"

$CLICKHOUSE_CLIENT -q "create table test (x UInt64, y UInt64 default 42) engine=File(TSKV, '02562_data.tskv') settings input_format_defaults_for_omitted_fields=1"
$CLICKHOUSE_CLIENT -q "select * from test"
$CLICKHOUSE_CLIENT -q "drop table test"

