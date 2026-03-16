SET parallel_replicas_local_plan = 1; -- this setting may skip index analysis when false
SET use_skip_indexes_on_data_read = 0;
SET materialize_skip_indexes_on_insert = 0;
SET mutations_sync = 2; -- disable asynchronous mutations

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    a UInt64,
    b UInt64,
    INDEX idx_a a TYPE minmax,
    INDEX `id,x_b` b TYPE set(3) -- weird but legal idx name just to make sure it works with setting
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4, materialize_skip_indexes_on_merge = 1;

-- negative test case
ALTER TABLE tab MODIFY SETTING exclude_materialize_skip_indexes_on_merge ='!@#$^#$&#$$%$,,.,3.45,45.';
INSERT INTO tab SELECT number, number / 50 FROM numbers(100);
OPTIMIZE TABLE tab FINAL; -- { serverError CANNOT_PARSE_TEXT }
TRUNCATE TABLE tab;

CREATE VIEW explain_indexes
AS SELECT trimLeft(explain) AS explain
FROM
(
    SELECT *
    FROM viewExplain('EXPLAIN', 'indexes = 1', (
        SELECT count()
        FROM tab
        WHERE (a >= 90) AND (a < 110) AND (b = 2)
    ))
)
WHERE (explain LIKE '%Name%') OR (explain LIKE '%Description%') OR (explain LIKE '%Parts%') OR (explain LIKE '%Granules%') OR (explain LIKE '%Range%');

SYSTEM STOP MERGES tab;
ALTER TABLE tab MODIFY SETTING exclude_materialize_skip_indexes_on_merge = 'idx_a';

INSERT INTO tab SELECT number, number / 50 FROM numbers(100);
INSERT INTO tab SELECT number, number / 50 FROM numbers(100, 100);

SELECT '';
SELECT 'idx_a is excluded, but we have not allowed a merge, so no filtering should occur'; 
SELECT * FROM explain_indexes;

SYSTEM START MERGES tab;
OPTIMIZE TABLE tab FINAL;

SELECT '';
SELECT 'After START MERGES and OPTIMIZE FINAL only idx_b should participate in filtering as idx_a is excluded';
SELECT * FROM explain_indexes;

ALTER TABLE tab MATERIALIZE INDEX idx_a;

SELECT '';
SELECT 'After explicit MATERIALIZE INDEX idx_a should also be materialized';
SELECT * FROM explain_indexes;

TRUNCATE TABLE tab;

ALTER TABLE tab MODIFY SETTING exclude_materialize_skip_indexes_on_merge = 'idx_a, `id,x_b`';

INSERT INTO tab SELECT number, number / 50 FROM numbers(100);
INSERT INTO tab SELECT number, number / 50 FROM numbers(100, 100);

OPTIMIZE TABLE tab FINAL;

SELECT '';
SELECT 'Both indexes are excluded, so neither should participate in filtering';
SELECT * FROM explain_indexes;

DROP TABLE tab;
DROP VIEW explain_indexes;

