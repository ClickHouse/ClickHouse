#!/usr/bin/env bash
# Tags: long, no-fasttest, no-ordinary-database
# Test vector index with random value of HNSW 'M' parameter.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "Starting the test"

# Typical M values seen in research and examples are 16/32/64 and maybe 128
# Also seen M=50 and M=100. Never seen odd values!
# Let's run this test with a random M between 2 and 200, even odd
# This test is to test usearch graph memory layout / alignment, serialization / deserialization

hnsw_m=`$CLICKHOUSE_CLIENT -q "SELECT (rand() % 199) + 2"`

res=`$CLICKHOUSE_CLIENT -mq "
    DROP TABLE IF EXISTS tab;

    CREATE TABLE tab
    (
        id Int32,
        vector Array(Float32),
        INDEX vector_index vector TYPE vector_similarity('hnsw','cosineDistance', 2, 'bf16', $hnsw_m, $hnsw_m)
    )
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS index_granularity=2;

    INSERT INTO tab SELECT number, [randCanonical(), randCanonical()] FROM numbers(300);

    SELECT id
    FROM tab
    ORDER BY cosineDistance(vector, [randCanonical(), randCanonical()])
    LIMIT 2;

    DROP TABLE tab;
"`

echo "Done"
