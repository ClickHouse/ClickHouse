#!/usr/bin/env bash
# Tags: long, no-fasttest, no-ordinary-database
#
# Test vector index with random values for HNSW parameters

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "Starting the test"

# Typical hnsw_max_connections_per_layer(M) values seen in research and examples are 16/32/64
# and maybe 128. Also seen M=50 and M=100. Never seen odd values!
# Let's run this test with a random M between 2 and 200, even odd.
# This test is to test usearch graph memory layout / alignment, serialization / deserialization

hnsw_max_connections_per_layer=`$CLICKHOUSE_CLIENT -q "SELECT (rand() % 199) + 2"`

# hnsw_candidate_list_size_for_construction must be >= hnsw_max_connections_per_layer
hnsw_candidate_list_size_for_construction=`$CLICKHOUSE_CLIENT -q "SELECT $hnsw_max_connections_per_layer + (rand() % 100)"`

# hnsw_candidate_list_size_for_search must also be >= hnsw_max_connections_per_layer
hnsw_candidate_list_size_for_search=`$CLICKHOUSE_CLIENT -q "SELECT $hnsw_max_connections_per_layer + (rand() % 100)"`

test_runtime_hnsw_settings="hnsw M $hnsw_max_connections_per_layer efc $hnsw_candidate_list_size_for_construction efsearch $hnsw_candidate_list_size_for_search"

res=`$CLICKHOUSE_CLIENT -mq "
    DROP TABLE IF EXISTS tab;

    CREATE TABLE tab
    (
        id Int32,
        vector Array(Float32),
	INDEX vector_index vector TYPE vector_similarity('hnsw','cosineDistance', 2, 'bf16', $hnsw_max_connections_per_layer, $hnsw_candidate_list_size_for_construction)
    )
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS index_granularity=2 /* $test_runtime_hnsw_settings */;

    INSERT INTO tab SELECT number, [randCanonical(), randCanonical()] FROM numbers(300) /* $test_runtime_hnsw_settings */;

    SELECT id
    FROM tab
    ORDER BY cosineDistance(vector, [randCanonical(), randCanonical()])
    LIMIT 2
    SETTINGS hnsw_candidate_list_size_for_search=$hnsw_candidate_list_size_for_search /* $test_runtime_hnsw_settings */;

    DROP TABLE tab;
"`

echo "Done"
