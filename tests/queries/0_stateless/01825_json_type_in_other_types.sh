#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The test prints whole `Map` values, whose key order `with_buckets` serialization does not
# preserve (keys are reassembled in hash-bucket order). Pin the serialization, which CI randomizes.
# `t_json_nested_buckets` below keeps `with_buckets` covered for this column type.
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_json_nested;

    CREATE TABLE t_json_nested
    (
        id UInt32,
        data Tuple(String, Map(String, Array(JSON)), JSON)
    )
    ENGINE = MergeTree ORDER BY id
    SETTINGS map_serialization_version = 'basic',
             map_serialization_version_for_zero_level_parts = 'basic'" --enable_json_type 1

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

# Same column type read back through the bucketed Map format. Since 26.8 a bucketed part
# carries a `bucket_indexes` substream and a whole-map read restores the written key order
# from it; parts without that substream still reassemble in bucket order. So the whole-map
# assertion below is deterministic here and fails if that restoration stops working.
# `serialization_info_version` and `max_buckets_in_map` are pinned as well: the first
# downgrades Map serialization to `basic` and the second collapses the column to a
# single bucket, and either one would silently skip the split/reassembly path.
#
# 4 is the smallest `max_buckets_in_map` that puts `aa` and `bb` in different buckets
# *and* reassembles them in the swapped order, i.e. exactly the layout that made the
# order-sensitive assertions above flaky. The default 32 adds no coverage here and
# writes 2801 substreams for this column instead of 393; re-reading all of them once
# per query costs ~53s of the test's ~65s under ASan + UBSan with S3 storage and meta
# in Keeper, which trips the flaky check's 180s-per-run limit.
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_json_nested_buckets;

    CREATE TABLE t_json_nested_buckets
    (
        id UInt32,
        data Tuple(String, Map(String, Array(JSON)), JSON)
    )
    ENGINE = MergeTree ORDER BY id
    SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets',
             serialization_info_version = 'with_types', map_buckets_strategy = 'constant',
             max_buckets_in_map = 4, map_buckets_min_avg_size = 1;

    INSERT INTO t_json_nested_buckets SELECT * FROM t_json_nested" --enable_json_type 1

# The key subscripts below are order-independent, so they would also pass on a map
# written without bucketing. Assert the part really has 4 key buckets first.
$CLICKHOUSE_CLIENT -q "
    SELECT countDistinct(extract(s, '^data%2E2\.([0-9]+)%2Ekeys\$')) AS buckets
    FROM (SELECT arrayJoin(substreams) AS s FROM system.parts_columns
          WHERE database = currentDatabase() AND table = 't_json_nested_buckets' AND column = 'data' AND active)
    WHERE match(s, '^data%2E2\.[0-9]+%2Ekeys\$')"

$CLICKHOUSE_CLIENT -q "
    SELECT id, data.1 AS s, data.2 AS m, data.3 AS obj
    FROM t_json_nested_buckets ORDER BY id FORMAT JSONEachRow"

$CLICKHOUSE_CLIENT -q "
    SELECT (data.2)['aa'] AS aa, (data.2)['bb'] AS bb
    FROM t_json_nested_buckets ORDER BY id FORMAT JSONEachRow"

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_json_nested_buckets;
    DROP TABLE IF EXISTS t_json_nested"
