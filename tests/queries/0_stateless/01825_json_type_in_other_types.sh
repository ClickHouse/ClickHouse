#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_json_nested"

# `basic` is pinned because `with_buckets` reassembles a Map in bucket order, so `SELECT *`
# would print the keys in a run-dependent order. `t_json_nested_buckets` below keeps
# `with_buckets` covered for this column type using order-independent queries.
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_json_nested
    (
        id UInt32,
        data Tuple(String, Map(String, Array(JSON)), JSON)
    )
    ENGINE = MergeTree ORDER BY id
    SETTINGS map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'basic'" --enable_json_type 1

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

$CLICKHOUSE_CLIENT -q "SELECT * FROM t_json_nested ORDER BY id FORMAT JSONEachRow"

echo "============="

$CLICKHOUSE_CLIENT -q "
    SELECT (data.2)['aa'] AS aa, (data.2)['bb'] AS bb
    FROM t_json_nested ORDER BY id FORMAT JSONEachRow"

echo "============="

$CLICKHOUSE_CLIENT --enable_analyzer=1 -q "
    WITH (data.2)['aa'] AS aa, (data.2)['bb'] AS bb
    SELECT aa.k1 AS k1,
           aa.k4 AS k4
    FROM t_json_nested ORDER BY id FORMAT JSONEachRow"

echo "============="

$CLICKHOUSE_CLIENT -q "SELECT data.3 AS obj FROM t_json_nested ORDER BY id FORMAT JSONEachRow"

echo "============="

# Same column type read back through the bucketed Map format. Every query here is
# order-independent, so the assertions hold under either bucket layout.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_json_nested_buckets"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_json_nested_buckets
    (
        id UInt32,
        data Tuple(String, Map(String, Array(JSON)), JSON)
    )
    ENGINE = MergeTree ORDER BY id
    SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets',
             map_buckets_strategy = 'constant', map_buckets_min_avg_size = 1" --enable_json_type 1

${CLICKHOUSE_CLIENT} -q "INSERT INTO t_json_nested_buckets SELECT * FROM t_json_nested"

$CLICKHOUSE_CLIENT -q "
    SELECT id, data.1 AS s, mapSort(data.2) AS m, data.3 AS obj
    FROM t_json_nested_buckets ORDER BY id FORMAT JSONEachRow"

$CLICKHOUSE_CLIENT -q "
    SELECT (data.2)['aa'] AS aa, (data.2)['bb'] AS bb
    FROM t_json_nested_buckets ORDER BY id FORMAT JSONEachRow"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_json_nested_buckets"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_json_nested"
