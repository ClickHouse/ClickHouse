#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$USER_FILES_PATH/data_02293

$CLICKHOUSE_CLIENT -q "drop table if exists test_02293" 
$CLICKHOUSE_CLIENT -q "create table test_02293 (a UInt32, b String, c Array(Tuple(Array(UInt32), String))) engine=Memory"
$CLICKHOUSE_CLIENT -q "insert into test_02293 select number, 'String', [(range(number % 3), 'String'), (range(number % 4), 'gnirtS')] from numbers(5) settings max_block_size=2"

echo "JSONColumns"
$CLICKHOUSE_CLIENT -q "select * from test_02293 order by a format JSONColumns"
$CLICKHOUSE_CLIENT -q "select * from test_02293 order by a format JSONColumns" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "desc file(data_02293, JSONColumns)"
$CLICKHOUSE_CLIENT -q "select * from file(data_02293, JSONColumns) order by a"

echo "JSONCompactColumns"
$CLICKHOUSE_CLIENT -q "select * from test_02293 order by a format JSONCompactColumns"
$CLICKHOUSE_CLIENT -q "select * from test_02293 order by a format JSONCompactColumns" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "desc file(data_02293, JSONCompactColumns)"
$CLICKHOUSE_CLIENT -q "select * from file(data_02293, JSONCompactColumns) order by c1, c2, c3"

echo "JSONColumnsWithMetadata"
$CLICKHOUSE_CLIENT -q "select sum(a) as sum, avg(a) as avg from test_02293 group by a % 4 with totals order by tuple(sum, avg) format JSONColumnsWithMetadata" --extremes=1 | grep -v "elapsed"


echo '
{
    "b": [1, 2, 3],
    "a": [3, 2, 1]
}
{
    "c": [1, 2, 3]
}
{
}
{
    "a": [],
    "d": []
}
{
    "d": ["String"]
}
' > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file(data_02293, JSONColumns)"
$CLICKHOUSE_CLIENT -q "select * from file(data_02293, JSONColumns) order by b, a, c, d"
$CLICKHOUSE_CLIENT -q "select * from file(data_02293, JSONColumns, 'a UInt32, t String') order by a, t settings input_format_skip_unknown_fields=0" 2>&1 | grep -F -q 'INCORRECT_DATA' && echo 'OK' || echo 'FAIL'
$CLICKHOUSE_CLIENT -q "select * from file(data_02293, JSONColumns, 'a UInt32, t String') order by a, t settings input_format_skip_unknown_fields=1"

echo '
[
    [1, 2, 3],
    [1, 2, 3]
]
[
    [1, 2, 3]
]
[
]
[
    [],
    []
]
[
    [1],
    [2],
    ["String"]
]
' > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file(data_02293, JSONCompactColumns)"
$CLICKHOUSE_CLIENT -q "select * from file(data_02293, JSONCompactColumns) order by c1, c2, c3"
$CLICKHOUSE_CLIENT -q "select * from file(data_02293, JSONCompactColumns, 'a UInt32, t UInt32') order by a, t" 2>&1 | grep -F -q 'INCORRECT_DATA' && echo 'OK' || echo 'FAIL'

echo '
{
    "a": [null, null, null],
    "b": [3, 2, 1]
}
{
    "a": [1, 2, 3]
}
' > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file(data_02293, JSONColumns) settings input_format_max_rows_to_read_for_schema_inference=3, input_format_json_infer_incomplete_types_as_strings=0" 2>&1 | grep -F -q 'CANNOT_EXTRACT_TABLE_STRUCTURE' && echo 'OK' || echo 'FAIL'
