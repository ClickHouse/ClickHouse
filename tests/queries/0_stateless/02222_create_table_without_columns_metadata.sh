#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


METADATA_PATH=$($CLICKHOUSE_CLIENT -q "select metadata_path from system.databases where name = '$CLICKHOUSE_DATABASE'")

$CLICKHOUSE_CLIENT -q "insert into table function file(data.jsonl, 'JSONEachRow', 'x UInt32 default 42, y String') select number as x, 'String' as y from numbers(10)"

$CLICKHOUSE_CLIENT -q "drop table if exists test"
$CLICKHOUSE_CLIENT -q "create table test engine=File(JSONEachRow, 'data.jsonl')"

cat $METADATA_PATH/test.sql | grep -v "UUID"

