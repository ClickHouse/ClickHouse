-- Provide list of secondary indexes to skip on insert
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;

CREATE TABLE t_skip_index_insert
(
    a UInt64,
    b UInt64,
    INDEX idx_a a TYPE minmax,
    INDEX `id,x_b` b TYPE set(3)
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;

INSERT INTO t_skip_index_insert SELECT number, number / 50 FROM numbers(100)
SETTINGS exclude_materialize_skip_indexes_on_insert='!@#$^#$&#$$%$,,.,3.45,45.';  -- { serverError CANNOT_PARSE_TEXT }

SET enable_analyzer = 1;
SET exclude_materialize_skip_indexes_on_insert='idx_a';

SYSTEM STOP MERGES t_skip_index_insert;

INSERT INTO t_skip_index_insert SELECT number, number / 50 FROM numbers(100);
INSERT INTO t_skip_index_insert SELECT number, number / 50 FROM numbers(100, 100);

SELECT 'idx_a is excluded, so it should perform no filtering, while `id,x_b` should perform filtering since it is included';
EXPLAIN indexes = 1 SELECT count() FROM t_skip_index_insert WHERE a >= 90 AND a < 110 AND b = 2;

SYSTEM START MERGES t_skip_index_insert;
OPTIMIZE TABLE t_skip_index_insert FINAL;

SELECT 'After START MERGES and OPTIMIZE TABLE both indexes should participate in filtering';
EXPLAIN indexes = 1 SELECT count() FROM t_skip_index_insert WHERE a >= 90 AND a < 110 AND b = 2;

TRUNCATE TABLE t_skip_index_insert;

INSERT INTO t_skip_index_insert SELECT number, number / 50 FROM numbers(100);
INSERT INTO t_skip_index_insert SELECT number, number / 50 FROM numbers(100, 100);

SET mutations_sync = 2;

ALTER TABLE t_skip_index_insert MATERIALIZE INDEX idx_a;
ALTER TABLE t_skip_index_insert MATERIALIZE INDEX `id,x_b`;

SELECT 'MATERIALIZE INDEX should also cause both indexes to participate in filtering despite exclude setting';
EXPLAIN indexes = 1 SELECT count() FROM t_skip_index_insert WHERE a >= 90 AND a < 110 AND b = 2;

SYSTEM FLUSH LOGS query_log;

SELECT count()
FROM system.query_log
WHERE current_database = currentDatabase()
    AND query LIKE 'INSERT INTO t_skip_index_insert SELECT%'
    AND type = 'QueryFinish';

TRUNCATE TABLE t_skip_index_insert;

SET exclude_materialize_skip_indexes_on_insert='idx_a,`id,x_b`';

SYSTEM STOP MERGES t_skip_index_insert;

INSERT INTO t_skip_index_insert SELECT number, number / 50 FROM numbers(100);
INSERT INTO t_skip_index_insert SELECT number, number / 50 FROM numbers(100, 100);

SELECT 'Both indexes are excluded, so neither should participate in filtering';
EXPLAIN indexes = 1 SELECT count() FROM t_skip_index_insert WHERE a >= 90 AND a < 110 AND b = 2;

SYSTEM START MERGES t_skip_index_insert;
OPTIMIZE TABLE t_skip_index_insert FINAL;

SELECT 'After START MERGES and OPTIMIZE TABLE both indexes should participate in filtering';
EXPLAIN indexes = 1 SELECT count() FROM t_skip_index_insert WHERE a >= 90 AND a < 110 AND b = 2;

TRUNCATE TABLE t_skip_index_insert;

INSERT INTO t_skip_index_insert SELECT number, number / 50 FROM numbers(100);
INSERT INTO t_skip_index_insert SELECT number, number / 50 FROM numbers(100, 100);

SET mutations_sync = 2;

ALTER TABLE t_skip_index_insert MATERIALIZE INDEX idx_a;
ALTER TABLE t_skip_index_insert MATERIALIZE INDEX `id,x_b`;

SELECT 'MATERIALIZE INDEX should also cause both indexes to participate in filtering despite exclude setting';
EXPLAIN indexes = 1 SELECT count() FROM t_skip_index_insert WHERE a >= 90 AND a < 110 AND b = 2;

DROP TABLE t_skip_index_insert;

SYSTEM FLUSH LOGS query_log;

SELECT count()
FROM system.query_log
WHERE current_database = currentDatabase()
    AND query LIKE 'INSERT INTO t_skip_index_insert SELECT%'
    AND type = 'QueryFinish';

