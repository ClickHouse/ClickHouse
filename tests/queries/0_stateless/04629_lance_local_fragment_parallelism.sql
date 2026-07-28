-- Tags: no-parallel
-- Multi-fragment Lance packs: correctness vs single-pack and ProfileEvents.

DROP TABLE IF EXISTS lance_local_fragment_parallelism;

CREATE TABLE lance_local_fragment_parallelism
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/multi_frag.lance');

-- Parallel on/off must agree on aggregates (streams are unordered).
SELECT count(), sum(id), sum(cityHash64(name))
FROM lance_local_fragment_parallelism
SETTINGS
    lance_enable_fragment_parallelism = 1,
    lance_fragment_pack_mode = 'auto',
    max_threads = 4;

SELECT count(), sum(id), sum(cityHash64(name))
FROM lance_local_fragment_parallelism
SETTINGS lance_enable_fragment_parallelism = 0;

-- mode=one with a pack cap still returns all rows.
SELECT count(), sum(id)
FROM lance_local_fragment_parallelism
SETTINGS
    lance_enable_fragment_parallelism = 1,
    lance_fragment_pack_mode = 'one',
    lance_max_fragment_packs = 4,
    max_threads = 8;

-- Filter pushdown remains correct under multi-pack.
SELECT count(), sum(id)
FROM lance_local_fragment_parallelism
WHERE id <= 10
SETTINGS
    lance_enable_fragment_parallelism = 1,
    max_threads = 4;

-- LIMIT row count is correct (forces single pack).
SELECT count()
FROM
(
    SELECT id
    FROM lance_local_fragment_parallelism
    LIMIT 7
    SETTINGS lance_enable_fragment_parallelism = 1, max_threads = 4
);

SET log_queries = 1;

SELECT count(), sum(id)
FROM lance_local_fragment_parallelism
FORMAT Null
SETTINGS
    lance_enable_fragment_parallelism = 1,
    lance_fragment_pack_mode = 'pack',
    lance_max_fragment_packs = 4,
    max_threads = 4,
    log_comment = 'lance_frag_multi_pack';

SELECT count()
FROM lance_local_fragment_parallelism
FORMAT Null
SETTINGS
    lance_enable_fragment_parallelism = 0,
    log_comment = 'lance_frag_single_pack';

SELECT count()
FROM lance_local_fragment_parallelism
FORMAT Null
SETTINGS log_comment = 'lance_frag_count_fast';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['LanceFragmentsListed'] >= 8,
    ProfileEvents['LanceFragmentPacks'] > 1,
    ProfileEvents['LancePlanScan'] > 1
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = 'lance_frag_multi_pack'
ORDER BY event_time_microseconds DESC
LIMIT 1
SETTINGS enable_parallel_replicas = 0;

SELECT
    ProfileEvents['LanceFragmentPacks'] = 1,
    ProfileEvents['LanceFragmentParallelismDisabled'] > 0
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = 'lance_frag_single_pack'
ORDER BY event_time_microseconds DESC
LIMIT 1
SETTINGS enable_parallel_replicas = 0;

-- count() fast path (or full scan) must account for all rows.
SELECT
    ProfileEvents['LanceCountRows'] > 0 OR ProfileEvents['LanceRowsRead'] > 0,
    ProfileEvents['LanceFragmentParallelismDisabled'] > 0 OR ProfileEvents['LanceFragmentPacks'] = 1
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = 'lance_frag_count_fast'
ORDER BY event_time_microseconds DESC
LIMIT 1
SETTINGS enable_parallel_replicas = 0;

DROP TABLE lance_local_fragment_parallelism;
