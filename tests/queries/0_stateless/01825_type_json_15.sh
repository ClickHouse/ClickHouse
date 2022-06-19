#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_json_15"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_json_15 (obj JSON) ENGINE = MergeTree ORDER BY tuple()" --allow_experimental_object_type 1

cat <<EOF | $CLICKHOUSE_CLIENT -q "INSERT INTO t_json_15 FORMAT JSONAsObject"
{
    "id": 1,
    "key_0":[
        {"key_1":{"key_2":[1, 2, 3],"key_8":"sffjx"},"key_10":65535,"key_0":-1},
        {"key_10":10.23,"key_0":922337203.685}
    ]
}
EOF

$CLICKHOUSE_CLIENT -q "SELECT DISTINCT toTypeName(obj) FROM t_json_15;"
$CLICKHOUSE_CLIENT -q "SELECT obj FROM t_json_15 ORDER BY obj.id FORMAT JSONEachRow" --output_format_json_named_tuples_as_objects 1
$CLICKHOUSE_CLIENT -q "SELECT \
    obj.key_0.key_1.key_2, \
    obj.key_0.key_1.key_8, \
    obj.key_0.key_10, \
    obj.key_0.key_0 \
FROM t_json_15 ORDER BY obj.id"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_json_15;"
