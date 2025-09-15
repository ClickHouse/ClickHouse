-- Provide list of secondary indexes to skip on insert
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_skip_index_insert;

-- TODO test weird idx names with commas and backticks

CREATE TABLE t_skip_index_insert
(
    a UInt64,
    b UInt64,
    INDEX idx_a a TYPE minmax,
    INDEX idx_b b TYPE set(3)
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;

SET enable_analyzer = 1;
SET exclude_materialize_skip_indexes_on_insert='idx_a';

SYSTEM STOP MERGES t_skip_index_insert;

INSERT INTO t_skip_index_insert SELECT number, number / 50 FROM numbers(100);
INSERT INTO t_skip_index_insert SELECT number, number / 50 FROM numbers(100, 100);

SELECT count() FROM t_skip_index_insert WHERE a >= 110 AND a < 130 AND b = 2;
EXPLAIN indexes = 1 SELECT count() FROM t_skip_index_insert WHERE a >= 110 AND a < 130 AND b = 2;

SYSTEM START MERGES t_skip_index_insert;
OPTIMIZE TABLE t_skip_index_insert FINAL;

SELECT count() FROM t_skip_index_insert WHERE a >= 110 AND a < 130 AND b = 2;
EXPLAIN indexes = 1 SELECT count() FROM t_skip_index_insert WHERE a >= 110 AND a < 130 AND b = 2;

TRUNCATE TABLE t_skip_index_insert;

INSERT INTO t_skip_index_insert SELECT number, number / 50 FROM numbers(100);
INSERT INTO t_skip_index_insert SELECT number, number / 50 FROM numbers(100, 100);

SET mutations_sync = 2;

ALTER TABLE t_skip_index_insert MATERIALIZE INDEX idx_a;
ALTER TABLE t_skip_index_insert MATERIALIZE INDEX idx_b;

SELECT count() FROM t_skip_index_insert WHERE a >= 110 AND a < 130 AND b = 2;
EXPLAIN indexes = 1 SELECT count() FROM t_skip_index_insert WHERE a >= 110 AND a < 130 AND b = 2;

SYSTEM FLUSH LOGS query_log;

-- TODO this sum() won't work because it will always change since one index is not skipped
SELECT count(), sum(ProfileEvents['MergeTreeDataWriterSkipIndicesCalculationMicroseconds'])
FROM system.query_log
WHERE current_database = currentDatabase()
    AND query LIKE 'INSERT INTO t_skip_index_insert SELECT%'
    AND type = 'QueryFinish';

TRUNCATE TABLE t_skip_index_insert;

SET exclude_materialize_skip_indexes_on_insert='idx_a,idx_b';

SYSTEM STOP MERGES t_skip_index_insert;

INSERT INTO t_skip_index_insert SELECT number, number / 50 FROM numbers(100);
INSERT INTO t_skip_index_insert SELECT number, number / 50 FROM numbers(100, 100);

SELECT count() FROM t_skip_index_insert WHERE a >= 110 AND a < 130 AND b = 2;
EXPLAIN indexes = 1 SELECT count() FROM t_skip_index_insert WHERE a >= 110 AND a < 130 AND b = 2;

SYSTEM START MERGES t_skip_index_insert;
OPTIMIZE TABLE t_skip_index_insert FINAL;

SELECT count() FROM t_skip_index_insert WHERE a >= 110 AND a < 130 AND b = 2;
EXPLAIN indexes = 1 SELECT count() FROM t_skip_index_insert WHERE a >= 110 AND a < 130 AND b = 2;

TRUNCATE TABLE t_skip_index_insert;

INSERT INTO t_skip_index_insert SELECT number, number / 50 FROM numbers(100);
INSERT INTO t_skip_index_insert SELECT number, number / 50 FROM numbers(100, 100);

SET mutations_sync = 2;

ALTER TABLE t_skip_index_insert MATERIALIZE INDEX idx_a;
ALTER TABLE t_skip_index_insert MATERIALIZE INDEX idx_b;

SELECT count() FROM t_skip_index_insert WHERE a >= 110 AND a < 130 AND b = 2;
EXPLAIN indexes = 1 SELECT count() FROM t_skip_index_insert WHERE a >= 110 AND a < 130 AND b = 2;

DROP TABLE IF EXISTS t_skip_index_insert;

SYSTEM FLUSH LOGS query_log;

SELECT count(), sum(ProfileEvents['MergeTreeDataWriterSkipIndicesCalculationMicroseconds'])
FROM system.query_log
WHERE current_database = currentDatabase()
    AND query LIKE 'INSERT INTO t_skip_index_insert SELECT%'
    AND type = 'QueryFinish';
