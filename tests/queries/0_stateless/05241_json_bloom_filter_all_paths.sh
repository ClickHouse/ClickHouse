#!/usr/bin/env bash
# `jsonbf_v1` covers the predicates that a `bloom_filter` index over `JSONAllPaths(json)` uses, from its per-granule path directory.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_json_bloom_filter_index=1"

$CLICKHOUSE_CLIENT -q "
CREATE TABLE json_bf_all_paths
(
    id UInt64,
    j JSON(max_dynamic_paths = 2, typed String),
    INDEX bf j TYPE jsonbf_v1() GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO json_bf_all_paths VALUES
    (1, '{\"a\":1}'),
    (2, '{\"b\":{\"c\":\"x\"}}'),
    (3, '{\"d\":[1,2]}'),
    (4, '{\"e\":null}'),
    (5, '{\"f\":{}}'),
    (6, '{\"g\":[{\"h\":1}]}'),
    (7, '{\"typed\":\"q\"}'),
    (8, '{\"i\":true,\"k\":{\"l\":1}}');
"

# Each line: predicate, rows with the index, rows without it, and granules selected out of 8.
while IFS= read -r predicate; do
    with_index=$($CLICKHOUSE_CLIENT -q "SELECT arraySort(groupArray(id)) FROM json_bf_all_paths WHERE $predicate")
    without_index=$($CLICKHOUSE_CLIENT -q "SELECT arraySort(groupArray(id)) FROM json_bf_all_paths WHERE $predicate SETTINGS use_skip_indexes = 0")
    granules=$($CLICKHOUSE_CLIENT -q "EXPLAIN indexes = 1 SELECT count() FROM json_bf_all_paths WHERE $predicate SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0" \
        | awk '/Name: bf/ { found = 1 } found && /Granules:/ { print $2; exit }')
    echo "$predicate	$with_index	$without_index	${granules:-not used}"
done <<'PREDICATES'
has(JSONAllPaths(j), 'a')
has(JSONAllPaths(j), 'b.c')
has(JSONAllPaths(j), 'b')
has(JSONAllPaths(j), 'd')
has(JSONAllPaths(j), 'e')
has(JSONAllPaths(j), 'g')
has(JSONAllPaths(j), 'k.l')
has(JSONAllPaths(j), 'zz')
has(JSONAllPaths(j), 'typed')
NOT has(JSONAllPaths(j), 'a')
has(JSONAllPaths(j), 'a') OR has(JSONAllPaths(j), 'zz')
hasAny(JSONAllPaths(j), ['a', 'k.l'])
hasAny(JSONAllPaths(j), ['a', 'typed'])
hasAll(JSONAllPaths(j), ['i', 'k.l'])
hasAll(JSONAllPaths(j), ['i', 'typed'])
indexOf(JSONAllPaths(j), 'a') > 0
indexOf(JSONAllPaths(j), 'k.l') = 2
indexOf(JSONAllPaths(j), 'a') = 0
arrayJoin(JSONAllPaths(j)) = 'b.c'
JSONAllPaths(j) IN (['a'], ['b.c'])
isNotNull(j.a)
j.b.c IS NOT NULL
isNotNull(j.a.:Int64)
isNotNull(j.zz)
j.b.c.:String IN ('x', 'y')
j.a::Int64 IN (1, 2)
j.a::Int64 IN (0, 1)
j.d::Array(Int64) = [1, 2]
j.b.c = 'x'
PREDICATES

$CLICKHOUSE_CLIENT -q "DROP TABLE json_bf_all_paths"
