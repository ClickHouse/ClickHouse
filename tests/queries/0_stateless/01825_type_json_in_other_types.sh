#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "SET allow_experimental_object_type = 1"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_json_nested"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_json_nested
    (
        id UInt32,
        data Tuple(String, Map(String, Array(Object('json'))), Object('json'))
    )
    ENGINE = MergeTree ORDER BY id" --allow_experimental_object_type 1

cat <<EOF | $CLICKHOUSE_CLIENT -q "INSERT INTO t_json_nested FORMAT JSONEachRow"
{
    "id": 1,
    "data":[
        "foo",
        {
            "aa": [
                {"k1": [{"k2": 1, "k3": 2}, {"k3": 3}]},
                {"k1": [{"k2": 4}, {"k3": 5}, {"k2": 6}], "k4": "qqq"}
            ],
            "bb": [
                 {"k4": "www"},
                 {"k1": [{"k2": 7, "k3": 8}, {"k2": 9, "k3": 10}, {"k2": 11, "k3": 12}]}
            ]
        },
        {"k1": "aa", "k2": {"k3": "bb", "k4": "c"}}
    ]
}
{
    "id": 2,
    "data":[
        "bar",
        {
            "aa": [
                {"k1": [{"k2": 13, "k3": 14}, {"k2": 15, "k3": 16}], "k4": "www"}
            ],
        },
        {}
    ]
}
EOF

cat <<EOF | $CLICKHOUSE_CLIENT -q "INSERT INTO t_json_nested FORMAT JSONEachRow"
{
    "id": 3,
    "data":[
        "some",
        {
            "aa": [
                {"k1": [{"k3": 20, "k5": "some"}]},
            ],
        },
        {"k1": "eee"}
    ]
}
EOF

$CLICKHOUSE_CLIENT -q "SELECT toTypeName(data) FROM t_json_nested LIMIT 1"

echo "============="

$CLICKHOUSE_CLIENT -q "SELECT * FROM t_json_nested ORDER BY id FORMAT JSONEachRow" --output_format_json_named_tuples_as_objects 1

echo "============="

$CLICKHOUSE_CLIENT -q "
    SELECT (data.2)['aa'] AS aa, (data.2)['bb'] AS bb
    FROM t_json_nested ORDER BY id FORMAT JSONEachRow" --output_format_json_named_tuples_as_objects 1

echo "============="

$CLICKHOUSE_CLIENT -q "
    WITH (data.2)['aa'] AS aa, (data.2)['bb'] AS bb
    SELECT tupleElement(aa, 'k1') AS k1,
           tupleElement(aa, 'k4') AS k4
    FROM t_json_nested ORDER BY id FORMAT JSONEachRow" --output_format_json_named_tuples_as_objects 1

echo "============="

$CLICKHOUSE_CLIENT -q "SELECT data.3 AS obj FROM t_json_nested ORDER BY id FORMAT JSONEachRow" --output_format_json_named_tuples_as_objects 1

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_json_nested"

