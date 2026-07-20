#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_json_16"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_json_16 (obj Object('json')) ENGINE = MergeTree ORDER BY tuple()" --allow_experimental_object_type 1

cat <<EOF | $CLICKHOUSE_CLIENT -q "INSERT INTO t_json_16 FORMAT JSONAsObject"
{
    "id": 1,
    "key_0":[
        {
        "key_1":[
            {
            "key_2":
                {
                    "key_3":[
                        {"key_4":255},
                        {"key_4":65535},
                        {"key_7":255,"key_6":3}
                    ],
                    "key_5":[
                        {"key_7":"nnpqx","key_6":1},
                        {"key_7":255,"key_6":3}
                    ]
                }
            }
        ]
        }
    ]
}
EOF

$CLICKHOUSE_CLIENT -q "SELECT DISTINCT toTypeName(obj) FROM t_json_16;"
$CLICKHOUSE_CLIENT -q "SELECT obj FROM t_json_16 ORDER BY obj.id FORMAT JSONEachRow" --output_format_json_named_tuples_as_objects 1
$CLICKHOUSE_CLIENT -q "SELECT \
    obj.key_0.key_1.key_2.key_3.key_4,
    obj.key_0.key_1.key_2.key_3.key_6,
    obj.key_0.key_1.key_2.key_3.key_7,
    obj.key_0.key_1.key_2.key_5.key_6, \
    obj.key_0.key_1.key_2.key_5.key_7
FROM t_json_16 ORDER BY obj.id"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_json_16;"
