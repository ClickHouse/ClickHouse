#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists tab_orc"
$CLICKHOUSE_CLIENT -q "create table tab_orc(id String, name Nullable(String), array Array(Nullable(String))) Engine=Memory"
$CLICKHOUSE_CLIENT -q "insert into tab_orc from infile '$CURDIR/data_orc/data_nulls_with_dictionary_v2_encoding.orc' FORMAT ORC"
$CLICKHOUSE_CLIENT -q "select * from tab_orc"
$CLICKHOUSE_CLIENT -q "select name from tab_orc"
$CLICKHOUSE_CLIENT -q "select array from tab_orc"
$CLICKHOUSE_CLIENT -q "drop table tab_orc"

$CLICKHOUSE_CLIENT -q "create table tab_orc(id String, name Nullable(String)) Engine=Memory"
$CLICKHOUSE_CLIENT -q "insert into tab_orc from infile '$CURDIR/data_orc/data_nulls_with_direct_v2_encoding.orc' FORMAT ORC"
$CLICKHOUSE_CLIENT -q "select * from tab_orc"
$CLICKHOUSE_CLIENT -q "select name from tab_orc"

$CLICKHOUSE_CLIENT -q "drop table tab_orc"
