#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_json_6;"

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_json_6 (data JSON) ENGINE = MergeTree ORDER BY tuple()" --allow_experimental_json_type 1

cat <<EOF | $CLICKHOUSE_CLIENT -q "INSERT INTO t_json_6 FORMAT JSONAsObject"
{
    "key": "v1",
    "out": [
        {
            "type": 0,
            "value": 1,
            "outputs": []
        },
        {
            "type": 0,
            "value": 2,
            "outputs": [
                {
                    "index": 1960131,
                    "n": 0
                }
            ]
        }
    ]
}
{
    "key": "v2",
    "out": [
        {
            "type": 1,
            "value": 4,
            "outputs": [
                {
                    "index": 1881212,
                    "n": 1
                }
            ]
        },
        {
            "type": 1,
            "value": 3
        }
    ]
}
EOF

$CLICKHOUSE_CLIENT -q "SELECT DISTINCT arrayJoin(JSONAllPathsWithTypes(data)) as path FROM t_json_6 order by path;"
$CLICKHOUSE_CLIENT -q "SELECT DISTINCT arrayJoin(JSONAllPathsWithTypes(arrayJoin(data.out[]))) as path FROM t_json_6 order by path;"
$CLICKHOUSE_CLIENT -q "SELECT DISTINCT arrayJoin(JSONAllPathsWithTypes(arrayJoin(arrayJoin(data.out[].outputs[])))) as path FROM t_json_6 order by path;"
$CLICKHOUSE_CLIENT -q "SELECT data.key, data.out[].type, data.out[].value, data.out[].outputs[].index, data.out[].outputs[].n FROM t_json_6 ORDER BY data.key"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_json_6;"
