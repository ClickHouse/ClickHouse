#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_json_12"

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_json_12 (obj JSON) ENGINE = MergeTree ORDER BY tuple()" --allow_experimental_object_type 1

cat <<EOF | $CLICKHOUSE_CLIENT -q "INSERT INTO t_json_12 FORMAT JSONAsObject"
{
    "id": 1,
    "key_0":[
        {
            "key_1":[
                {
                    "key_3":[
                        {"key_7":1025,"key_6":25.5,"key_4":1048576,"key_5":0.0001048576},
                        {"key_7":2,"key_6":"","key_4":null}
                    ]
                }
            ]
        },
        {},
        {
            "key_1":[
                {
                    "key_3":[
                        {"key_7":-922337203685477580.8,"key_6":"aqbjfiruu","key_5":-1},
                        {"key_7":65537,"key_6":"","key_4":""}
                    ]
                },
                {
                    "key_3":[
                        {"key_7":21474836.48,"key_4":"ghdqyeiom","key_5":1048575}
                    ]
                }
            ]
        }
    ]
}
EOF

$CLICKHOUSE_CLIENT -q "SELECT DISTINCT toTypeName(obj) FROM t_json_12;"
$CLICKHOUSE_CLIENT -q "SELECT obj FROM t_json_12 ORDER BY obj.id FORMAT JSONEachRow" --output_format_json_named_tuples_as_objects 1
$CLICKHOUSE_CLIENT -q "SELECT obj.key_0.key_1.key_3.key_4, obj.key_0.key_1.key_3.key_5, \
    obj.key_0.key_1.key_3.key_6, obj.key_0.key_1.key_3.key_7 FROM t_json_12 ORDER BY obj.id"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_json_12;"
