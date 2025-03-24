#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_json_11"

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_json_11 (obj JSON) ENGINE = MergeTree ORDER BY tuple()" --allow_experimental_json_type 1

cat <<EOF | $CLICKHOUSE_CLIENT -q "INSERT INTO t_json_11 FORMAT JSONAsObject"
{
    "id": 1,
    "key_1":[
        {
            "key_2":100,
            "key_3": [
                {
                    "key_7":257,
                    "key_4":[{"key_5":-2}]
                }
            ]
        },
        {
            "key_2":65536
        }
    ]
}
{
    "id": 2,
    "key_1":[
        {
          "key_2":101,
          "key_3": [
              {
                  "key_4":[{"key_5":-2}]
              }
          ]
        },
        {
            "key_2":102,
            "key_3": [
                {
                    "key_7":257
                }
            ]
        },
        {
            "key_2":65536
        }
    ]
}
EOF

$CLICKHOUSE_CLIENT -q "SELECT DISTINCT arrayJoin(JSONAllPathsWithTypes(obj)) as path FROM t_json_11 order by path;"
$CLICKHOUSE_CLIENT -q "SELECT DISTINCT arrayJoin(JSONAllPathsWithTypes(arrayJoin(obj.key_1[]))) as path FROM t_json_11 order by path;"
$CLICKHOUSE_CLIENT -q "SELECT DISTINCT arrayJoin(JSONAllPathsWithTypes(arrayJoin(arrayJoin(obj.key_1[].key_3[])))) as path FROM t_json_11 order by path;"
$CLICKHOUSE_CLIENT -q "SELECT DISTINCT arrayJoin(JSONAllPathsWithTypes(arrayJoin(arrayJoin(arrayJoin(obj.key_1[].key_3[].key_4[]))))) as path FROM t_json_11 order by path;"
$CLICKHOUSE_CLIENT -q "SELECT obj FROM t_json_11 ORDER BY obj.id FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT -q "SELECT obj.key_1[].key_3 FROM t_json_11 ORDER BY obj.id FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT -q "SELECT obj.key_1[].key_3[].key_4[].key_5, obj.key_1[].key_3[].key_7 FROM t_json_11 ORDER BY obj.id"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_json_11;"
