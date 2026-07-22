#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database, no-parallel, no-random-settings
# no-parallel: checks server-wide VectorSimilarityIndexCacheBytes metric
# no-random-settings: old_parts_lifetime = 0 must not be overridden

# Regression test: verify that VectorSimilarityIndexCache entries are evicted
# when old parts are removed after a mutation (not only on DROP TABLE).
# Uses S3 storage (MinIO) so the cache-key mismatch between getFullPath() and
# getRelativePathOfActivePart() manifests, allowing bugfix validation to
# reproduce the bug on unpatched master.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS tab"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP VECTOR SIMILARITY INDEX CACHE"

# Cache must be empty after explicit drop
$CLICKHOUSE_CLIENT --query "SELECT metric, value FROM system.metrics WHERE metric = 'VectorSimilarityIndexCacheBytes'"

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE tab (
        id Int32,
        vec Array(Float32),
        INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)
    )
    ENGINE = MergeTree ORDER BY id
    SETTINGS index_granularity = 8192, old_parts_lifetime = 0, merge_tree_clear_old_parts_interval_seconds = 1, cleanup_delay_period = 0, cleanup_delay_period_random_add = 0, cleanup_thread_preferred_points_per_iteration = 0, storage_policy = 's3_no_cache'
"

$CLICKHOUSE_CLIENT --query "
    INSERT INTO tab VALUES
        (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]),
        (5, [0.0, 2.0]), (6, [0.0, 2.1]), (7, [0.0, 2.2]), (8, [0.0, 2.3]), (9, [0.0, 2.4])
"

# Populate vector similarity index cache
$CLICKHOUSE_CLIENT --query "
    WITH [0.0, 2.0] AS reference_vec
    SELECT id FROM tab ORDER BY L2Distance(vec, reference_vec) LIMIT 3
"

# Record cache size — must be non-empty
cache_before=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.metrics WHERE metric = 'VectorSimilarityIndexCacheBytes'")
if [ "$cache_before" -gt 0 ]; then
    echo "VectorSimilarityIndexCacheBytes	Populated"
else
    echo "VectorSimilarityIndexCacheBytes	UNEXPECTED_ZERO"
fi

# Mutation replaces the part; the old part becomes Outdated
$CLICKHOUSE_CLIENT --query "ALTER TABLE tab DELETE WHERE id = 0 SETTINGS mutations_sync = 2"

# Wait for the old part's cache entries to be evicted.
# We poll the cache metric directly because system.parts count drops to 1
# when grabOldParts() transitions the part to Deleting state, but clearCaches()
# runs later during clearPartsFromFilesystemImpl(). Checking system.parts first
# would introduce a race between the state transition and the actual eviction.
cache_decreased=0
for _ in $(seq 1 60); do
    cache_after=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.metrics WHERE metric = 'VectorSimilarityIndexCacheBytes'")
    if [ "$cache_after" -lt "$cache_before" ]; then
        cache_decreased=1
        break
    fi
    sleep 1
done

if [ "$cache_decreased" -eq 1 ]; then
    echo "VectorSimilarityIndexCacheBytes	Decreased"
else
    echo "VectorSimilarityIndexCacheBytes	NOT_DECREASED (before=$cache_before, after=$cache_after)"
fi

$CLICKHOUSE_CLIENT --query "DROP TABLE tab SYNC"
