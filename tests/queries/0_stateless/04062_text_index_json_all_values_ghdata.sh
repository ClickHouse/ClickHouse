#!/usr/bin/env bash
# Tags: no-parallel-replicas, no-fasttest, no-object-storage, long

# Tests that text indexes built on JSONAllValues work correctly on a real-world
# GitHub Events dataset (ghdata_sample.json) with various query patterns.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

MY_CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --enable_analyzer 1"

function run_query()
{
    local query=$1
    echo "$query"
    $MY_CLICKHOUSE_CLIENT --query "$query"

    $MY_CLICKHOUSE_CLIENT --query "
        SELECT trimLeft(explain) FROM (
            EXPLAIN indexes = 1 $query
        )
        WHERE explain LIKE '%Condition:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
        LIMIT 3;
    "
}

function run_query_no_idx()
{
    local query=$1
    $MY_CLICKHOUSE_CLIENT --query "$query" --use_skip_indexes_on_data_read=0
}

$MY_CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS ghdata;

    CREATE TABLE ghdata
    (
        data JSON(max_dynamic_paths=100),
        INDEX json_idx JSONAllValues(data) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lowerUTF8(JSONAllValues(data)))
    )
    ENGINE = MergeTree
    ORDER BY tuple()
    SETTINGS index_granularity = 100, index_granularity_bytes = '10M';
"

cat "$CUR_DIR"/data_json/ghdata_sample.json | $MY_CLICKHOUSE_CLIENT \
    --max_memory_usage 10G --query "INSERT INTO ghdata FORMAT JSONAsObject"

$MY_CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE ghdata FINAL;"

echo "-- Point lookup by string value"
run_query "SELECT count() FROM ghdata WHERE data.actor.login = 'dependabot[bot]'"

echo "-- Point lookup by numeric value"
run_query "SELECT count() FROM ghdata WHERE data.repo.id = 323586805"

echo "-- Equality on event type"
run_query "SELECT count() FROM ghdata WHERE data.type = 'WatchEvent'"

echo "-- Equality on nested path"
run_query "SELECT count() FROM ghdata WHERE data.payload.action = 'opened'"

echo "-- LIKE pattern matching (index cannot help, all granules scanned)"
run_query "SELECT count() FROM ghdata WHERE data.repo.name LIKE '%python%'"

echo "-- hasToken on string field"
run_query "SELECT count() FROM ghdata WHERE hasToken(data.actor.login, 'dependabot')"

echo "-- hasAllTokens on string field with cast"
run_query "SELECT count() FROM ghdata WHERE hasAllTokens(data.payload.pull_request.title::String, 'Fix spelling')"

echo "-- hasAnyTokens on string field with cast"
run_query "SELECT count() FROM ghdata WHERE hasAnyTokens(data.payload.pull_request.title::String, ['Fix', 'fix'])"

echo "-- IN operator on string field (rare values, granules should be skipped)"
run_query "SELECT count() FROM ghdata WHERE data.type::String IN ('MemberEvent', 'ForkEvent')"

echo "-- IN operator on nested path (rare values, granules should be skipped)"
run_query "SELECT count() FROM ghdata WHERE data.payload.action::String IN ('reopened', 'published')"

echo "-- Combined conditions on different paths"
run_query "SELECT count() FROM ghdata WHERE data.type = 'WatchEvent' AND data.repo.name = 'leonardomso/33-js-concepts'"

echo "-- Schema-agnostic search with JSONAllValues and hasAllTokens"
run_query "SELECT count() FROM ghdata WHERE hasAllTokens(JSONAllValues(data), ['football', 'team'])"

echo "-- Schema-agnostic search with JSONAllValues and hasAnyTokens"
run_query "SELECT count() FROM ghdata WHERE hasAnyTokens(JSONAllValues(data), ['football', 'calculator'])"

echo "-- Array: has() on nested array of labels in pull requests"
run_query "SELECT count() FROM ghdata WHERE has(data.payload.pull_request.labels[].name::Array(String), 'bug')"

echo "-- Array: has() on nested array of labels in issues"
run_query "SELECT count() FROM ghdata WHERE has(data.payload.issue.labels[].name::Array(String), 'bug')"

echo "-- Array: has() with a less common label"
run_query "SELECT count() FROM ghdata WHERE has(data.payload.pull_request.labels[].name::Array(String), 'enhancement')"

echo "-- Array: equality on array subcolumn"
run_query "SELECT count() FROM ghdata WHERE data.payload.pull_request.labels[].name = ['dependencies', 'submodules']"

echo "-- SELECT JSONAllValues with filter returning 1 row"
run_query "SELECT length(JSONAllPaths(data)), cityHash64(JSONAllPaths(data)), length(JSONAllValues(data)), cityHash64(JSONAllValues(data)) FROM ghdata WHERE data.id::UInt64 = 14690746673"

echo "-- Verify correctness: results match with index disabled"
run_query_no_idx "SELECT count() FROM ghdata WHERE data.actor.login = 'dependabot[bot]'"
run_query_no_idx "SELECT count() FROM ghdata WHERE data.repo.id = 323586805"
run_query_no_idx "SELECT count() FROM ghdata WHERE data.type = 'WatchEvent'"
run_query_no_idx "SELECT count() FROM ghdata WHERE data.payload.action = 'opened'"
run_query_no_idx "SELECT count() FROM ghdata WHERE data.repo.name LIKE '%python%'"
run_query_no_idx "SELECT count() FROM ghdata WHERE hasToken(data.actor.login, 'dependabot')"
run_query_no_idx "SELECT count() FROM ghdata WHERE hasAllTokens(data.payload.pull_request.title::String, 'Fix spelling')"
run_query_no_idx "SELECT count() FROM ghdata WHERE hasAnyTokens(data.payload.pull_request.title::String, ['Fix', 'fix'])"
run_query_no_idx "SELECT count() FROM ghdata WHERE data.type::String IN ('MemberEvent', 'ForkEvent')"
run_query_no_idx "SELECT count() FROM ghdata WHERE data.payload.action::String IN ('reopened', 'published')"
run_query_no_idx "SELECT count() FROM ghdata WHERE data.type = 'WatchEvent' AND data.repo.name = 'leonardomso/33-js-concepts'"
run_query_no_idx "SELECT count() FROM ghdata WHERE hasAllTokens(JSONAllValues(data), ['football', 'team'])"
run_query_no_idx "SELECT count() FROM ghdata WHERE hasAnyTokens(JSONAllValues(data), ['football', 'calculator'])"
run_query_no_idx "SELECT count() FROM ghdata WHERE has(data.payload.pull_request.labels[].name::Array(String), 'bug')"
run_query_no_idx "SELECT count() FROM ghdata WHERE has(data.payload.issue.labels[].name::Array(String), 'bug')"
run_query_no_idx "SELECT count() FROM ghdata WHERE has(data.payload.pull_request.labels[].name::Array(String), 'enhancement')"
run_query_no_idx "SELECT count() FROM ghdata WHERE data.payload.pull_request.labels[].name = ['dependencies', 'submodules']"
run_query_no_idx "SELECT length(JSONAllPaths(data)), cityHash64(JSONAllPaths(data)), length(JSONAllValues(data)), cityHash64(JSONAllValues(data)) FROM ghdata WHERE data.id::UInt64 = 14690746673"

$MY_CLICKHOUSE_CLIENT --query "DROP TABLE ghdata;"
