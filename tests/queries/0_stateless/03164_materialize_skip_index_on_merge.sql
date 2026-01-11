-- Tests merge tree 'setting' materialize_skip_indexes_on_merge

SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    a UInt64,
    b UInt64,
    INDEX idx_a a TYPE minmax,
    INDEX idx_b b TYPE set(3)
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;

SELECT 'Regular merge';

INSERT INTO tab SELECT number, number / 50 FROM numbers(100);
INSERT INTO tab SELECT number, number / 50 FROM numbers(100, 100);

OPTIMIZE TABLE tab FINAL;

SELECT count() FROM tab WHERE a >= 110 AND a < 130 AND b = 2;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1 SELECT count() FROM tab WHERE a >= 110 AND a < 130 AND b = 2
)
WHERE explain LIKE '%Skip%' OR explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

SELECT database, table, name, data_compressed_bytes FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab';

SELECT 'Merge with materialize_skip_indexes_on_merge = 1';

ALTER TABLE tab MODIFY SETTING materialize_skip_indexes_on_merge = 0;

TRUNCATE tab;
INSERT INTO tab SELECT number, number / 50 FROM numbers(100);
INSERT INTO tab SELECT number, number / 50 FROM numbers(100, 100);

OPTIMIZE TABLE tab FINAL;

SELECT count() FROM tab WHERE a >= 110 AND a < 130 AND b = 2;

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1 SELECT count() FROM tab WHERE a >= 110 AND a < 130 AND b = 2
)
WHERE explain LIKE '%Skip%' OR explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

SELECT database, table, name, data_compressed_bytes FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab';

SYSTEM FLUSH LOGS query_log;
SELECT count(), sum(ProfileEvents['MergeTreeDataWriterSkipIndicesCalculationMicroseconds'])
FROM system.query_log
WHERE current_database = currentDatabase()
    AND query LIKE 'OPTIMIZE TABLE tab FINAL'
    AND type = 'QueryFinish';

SELECT 'Materialize indexes explicitly';

SET mutations_sync = 2;
ALTER TABLE tab MATERIALIZE INDEX idx_a;
ALTER TABLE tab MATERIALIZE INDEX idx_b;

SELECT count() FROM tab WHERE a >= 110 AND a < 130 AND b = 2;
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1 SELECT count() FROM tab WHERE a >= 110 AND a < 130 AND b = 2
)
WHERE explain LIKE '%Skip%' OR explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT database, table, name, data_compressed_bytes FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab';

SELECT 'Merge after resetting materialize_skip_indexes_on_merge to default';

ALTER TABLE tab RESET SETTING materialize_skip_indexes_on_merge;

INSERT INTO tab SELECT number, number / 50 FROM numbers(100);
INSERT INTO tab SELECT number, number / 50 FROM numbers(100, 100);

OPTIMIZE TABLE tab FINAL;

SELECT count() FROM tab WHERE a >= 110 AND a < 130 AND b = 2;
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1 SELECT count() FROM tab WHERE a >= 110 AND a < 130 AND b = 2
)
WHERE explain LIKE '%Skip%' OR explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT database, table, name, data_compressed_bytes FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab';

DROP TABLE tab;
