#!/usr/bin/env bash
# Tags: no-parallel-replicas, no-old-analyzer
# Tag no-parallel-replicas: single-node test; parallel replicas relocate index analysis and the
#                           query condition cache lookups, so the per-part counts below do not hold
# Tag no-old-analyzer: the old analyzer never reaches the query condition cache, so there is
#                      nothing to count here

# With no effective skip index the profiled query condition cache key equals the bare one, so
# filterPartsByQueryConditionCache consults one key per part instead of two. Counting the cache's
# own TEST-level lookup lines is the only observable form of that: the two keys are equal, so the
# redundant lookup returns the same entry, merges idempotently and is not counted separately by
# QueryConditionCacheHits (one hit is reported per consultation, not per key).
# Both directions are pinned: the unindexed table must reach one lookup per part while the indexed
# one, where the profiled key really differs, must stay at two.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Merges are stopped so the part count stays fixed; the counts below are per part.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS qcc_lookup_plain;
    DROP TABLE IF EXISTS qcc_lookup_indexed;
    CREATE TABLE qcc_lookup_plain (a UInt64, b UInt64)
    ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 8, min_bytes_for_wide_part = 0, auto_statistics_types = '';
    CREATE TABLE qcc_lookup_indexed (a UInt64, b UInt64, INDEX bx b TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 8, min_bytes_for_wide_part = 0, auto_statistics_types = '';
    SYSTEM STOP MERGES qcc_lookup_plain;
    SYSTEM STOP MERGES qcc_lookup_indexed;
    INSERT INTO qcc_lookup_plain SELECT number, number % 100 FROM numbers(400);
    INSERT INTO qcc_lookup_plain SELECT number + 1000, number % 100 FROM numbers(400);
    INSERT INTO qcc_lookup_indexed SELECT number, number % 100 FROM numbers(400);
    INSERT INTO qcc_lookup_indexed SELECT number + 1000, number % 100 FROM numbers(400);
"

# Each setting is pinned in the query because the runner randomizes it and the wrong value would
# make the counts describe a different path:
#   use_query_condition_cache = 1        -- 0 never consults the cache at all
#   optimize_move_to_prewhere = 0,
#   query_plan_optimize_prewhere = 0     -- PREWHERE adds its own predicate, hence its own lookups
#   max_block_size = 8 (= index_granularity) -- lets the row-level writer fill the bare key too
#   use_skip_indexes_for_disjunctions    -- part of the profiled key's salt
#   enable_parallel_replicas = 0         -- 1 moves the lookups to the replica cluster
# ast_fuzzer_runs = 0 keeps the stress profile's server-side fuzzer from re-running the query,
# whose re-execution would add its own lookup lines.
settings="use_query_condition_cache = 1, ast_fuzzer_runs = 0, optimize_move_to_prewhere = 0,
    query_plan_optimize_prewhere = 0, max_block_size = 8,
    use_skip_indexes_for_disjunctions = 1, enable_parallel_replicas = 0"

# `send_logs_level=test` makes the server evaluate the cache's TEST-level lookup logs and ship them
# to this client regardless of the server's own log level, so the count does not depend on the
# server configuration. Counting hits AND misses together makes the number independent of whether a
# concurrent test dropped the cache in between, so no no-parallel tag is needed here.
# The count only describes a completed read, so a query that threw must not reach it: the client's
# status is checked before the lines are counted.
lookups() {
    local logs
    logs=$($CLICKHOUSE_CLIENT --send_logs_level=test -q "
        SELECT sum(b) FROM $1 WHERE a > 1200 AND b = 7 SETTINGS $settings
    " 2>&1 >/dev/null) || { echo "query on $1 failed:" >&2; grep -F 'Received exception' -A1 <<< "$logs" >&2; return 1; }
    grep -cE 'QueryConditionCache: (Read|Could not find) entry' <<< "$logs"
}

parts() {
    $CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.parts
        WHERE database = currentDatabase() AND table = '$1' AND active
    "
}

# One lookup per part with no effective skip index, two when one really ran. Dividing by the part
# count keeps both arms honest if the fixture ever produces a different number of parts.
echo "$(( $(lookups qcc_lookup_plain) / $(parts qcc_lookup_plain) ))"
echo "$(( $(lookups qcc_lookup_indexed) / $(parts qcc_lookup_indexed) ))"

# The indexed arm above only bounds the profiled key's lookup if the index was really used.
$CLICKHOUSE_CLIENT -q "
    SELECT count() > 0 FROM
    (
        EXPLAIN indexes = 1 SELECT sum(b) FROM qcc_lookup_indexed WHERE a > 1200 AND b = 7
    )
    WHERE explain ILIKE '%bx%';
"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE qcc_lookup_plain;
    DROP TABLE qcc_lookup_indexed;
"
