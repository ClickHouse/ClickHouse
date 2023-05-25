#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_json_13"

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_json_13 (obj JSON) ENGINE = MergeTree ORDER BY tuple()" --allow_experimental_object_type 1

cat <<EOF | $CLICKHOUSE_CLIENT -q "INSERT INTO t_json_13 FORMAT JSONAsObject"
{
    "id": 1,
    "key_1":[
        {
            "key_2":[
                {
                    "key_3":[
                        {"key_8":65537},
                        {
                            "key_4":[
                                {"key_5":-0.02},
                                {"key_7":1023},
                                {"key_7":1,"key_6":9223372036854775807}
                            ]
                        },
                        {
                            "key_4":[{"key_7":65537,"key_6":null}]
                        }
                    ]
                }
            ]
        }
    ]
}
EOF

$CLICKHOUSE_CLIENT -q "SELECT DISTINCT toTypeName(obj) FROM t_json_13;"
$CLICKHOUSE_CLIENT -q "SELECT obj FROM t_json_13 ORDER BY obj.id FORMAT JSONEachRow" --output_format_json_named_tuples_as_objects 1
$CLICKHOUSE_CLIENT -q "SELECT \
    obj.key_1.key_2.key_3.key_8, \
    obj.key_1.key_2.key_3.key_4.key_5, \
    obj.key_1.key_2.key_3.key_4.key_6, \
    obj.key_1.key_2.key_3.key_4.key_7 \
FROM t_json_13 ORDER BY obj.id"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_json_13;"
