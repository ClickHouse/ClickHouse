#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "select toFloat32('+42.42'), toFloat64('+42.42')"
$CLICKHOUSE_CLIENT -q "drop table if exists test_02127"
$CLICKHOUSE_CLIENT -q "create table test_02127 (x Float32, y Float64) engine=Memory()"

for escaping_rule in Quoted JSON Escaped CSV Raw
do
echo -e "+42.42\t+42.42" | $CLICKHOUSE_CLIENT -q "insert into test_02127 settings format_custom_escaping_rule='$escaping_rule' format CustomSeparated"
done


$CLICKHOUSE_CLIENT -q "select * from test_02127"
$CLICKHOUSE_CLIENT -q "drop table test_02127"
