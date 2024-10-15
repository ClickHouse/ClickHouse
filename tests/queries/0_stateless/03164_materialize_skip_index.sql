DROP TABLE IF EXISTS t_skip_index_insert;

CREATE TABLE t_skip_index_insert
(
    a UInt64,
    b UInt64,
    INDEX idx_a a TYPE minmax,
    INDEX idx_b b TYPE set(3)
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;

SET enable_analyzer = 1;
SET materialize_skip_indexes_on_insert = 0;

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

SYSTEM FLUSH LOGS;

SELECT count(), sum(ProfileEvents['MergeTreeDataWriterSkipIndicesCalculationMicroseconds'])
FROM system.query_log
WHERE current_database = currentDatabase()
    AND query LIKE 'INSERT INTO t_skip_index_insert SELECT%'
    AND type = 'QueryFinish';
