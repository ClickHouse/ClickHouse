-- Tags: no-parallel

DROP TABLE IF EXISTS test_skip_index_log;

CREATE TABLE test_skip_index_log
(
    id UInt32,
    value String,
    num UInt32,
    INDEX idx_minmax num TYPE minmax GRANULARITY 1,
    INDEX idx_set num TYPE set(100) GRANULARITY 1,
    INDEX idx_bf value TYPE bloom_filter GRANULARITY 1,
    INDEX idx_ngram value TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY id;

INSERT INTO test_skip_index_log SELECT number, toString(number), number FROM numbers(1000);

-- Query that uses minmax and set indexes (numeric condition)
SELECT count() FROM test_skip_index_log WHERE num > 500 FORMAT Null;

SYSTEM FLUSH LOGS;

SELECT arraySort(used_index_types)
FROM system.query_log
WHERE current_database = currentDatabase()
    AND query LIKE '%FROM test_skip_index_log WHERE num > 500%'
    AND type = 'QueryFinish'
    AND query NOT LIKE '%system.query_log%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- Query that uses bloom_filter and ngrambf_v1 indexes (string condition)
SELECT count() FROM test_skip_index_log WHERE value = '42' FORMAT Null;

SYSTEM FLUSH LOGS;

SELECT arraySort(used_index_types)
FROM system.query_log
WHERE current_database = currentDatabase()
    AND query LIKE '%FROM test_skip_index_log WHERE value = %'
    AND type = 'QueryFinish'
    AND query NOT LIKE '%system.query_log%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- Query without skip indexes has empty array
SELECT count() FROM test_skip_index_log FORMAT Null;

SYSTEM FLUSH LOGS;

SELECT used_index_types
FROM system.query_log
WHERE current_database = currentDatabase()
    AND query LIKE '%count() FROM test_skip_index_log FORMAT Null%'
    AND type = 'QueryFinish'
    AND query NOT LIKE '%system.query_log%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE test_skip_index_log;

-- Test deduplication: two minmax indexes on different columns should produce a single 'minmax' entry
DROP TABLE IF EXISTS test_skip_index_dedup;

CREATE TABLE test_skip_index_dedup
(
    id UInt32,
    a UInt32,
    b UInt32,
    INDEX idx_mm_a a TYPE minmax GRANULARITY 1,
    INDEX idx_mm_b b TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY id;

INSERT INTO test_skip_index_dedup SELECT number, number, number FROM numbers(1000);

SELECT count() FROM test_skip_index_dedup WHERE a > 500 AND b < 200 FORMAT Null;

SYSTEM FLUSH LOGS;

SELECT arraySort(used_index_types)
FROM system.query_log
WHERE current_database = currentDatabase()
    AND query LIKE '%FROM test_skip_index_dedup WHERE a > 500 AND b < 200%'
    AND type = 'QueryFinish'
    AND query NOT LIKE '%system.query_log%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE test_skip_index_dedup;

-- Test that indexes defined on columns not referenced in WHERE are not recorded
DROP TABLE IF EXISTS test_skip_index_not_useful;

CREATE TABLE test_skip_index_not_useful
(
    id UInt32,
    indexed_col String,
    other_col UInt32,
    INDEX idx_bf indexed_col TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY id;

INSERT INTO test_skip_index_not_useful SELECT number, toString(number), number FROM numbers(1000);

-- Filter on other_col which has no skip index; bloom_filter on indexed_col should not be useful
SELECT count() FROM test_skip_index_not_useful WHERE other_col > 500 FORMAT Null;

SYSTEM FLUSH LOGS;

SELECT used_index_types
FROM system.query_log
WHERE current_database = currentDatabase()
    AND query LIKE '%FROM test_skip_index_not_useful WHERE other_col%'
    AND type = 'QueryFinish'
    AND query NOT LIKE '%system.query_log%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE test_skip_index_not_useful;
