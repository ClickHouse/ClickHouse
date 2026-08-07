-- Tags: no-old-analyzer

-- The pipeline of a block nested loop join: the right input is materialized by
-- `BlockNestedLoopBuild` streams first, and only then may the `BlockNestedLoopProbe` streams pull a
-- row, which `DelayedPorts` enforces. Both sides keep `max_threads` streams.

SET join_algorithm = 'direct,parallel_hash,hash';
SET max_threads = 3;
-- A swapped join would put the probe side on the right and change every count below.
SET query_plan_join_swap_table = 'false';

DROP TABLE IF EXISTS bnl_pipeline_l;
DROP TABLE IF EXISTS bnl_pipeline_r;

CREATE TABLE bnl_pipeline_l (id Int32, x Int32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE bnl_pipeline_r (id Int32, y Int32) ENGINE = MergeTree ORDER BY id;
INSERT INTO bnl_pipeline_l VALUES (1, 1), (2, 2);
INSERT INTO bnl_pipeline_r VALUES (1, 3), (2, 4);

SELECT 'build', count() FROM (EXPLAIN PIPELINE SELECT * FROM bnl_pipeline_l l LEFT JOIN bnl_pipeline_r r ON l.x < r.y)
WHERE explain LIKE '%BlockNestedLoopBuild × 3%';
SELECT 'probe', count() FROM (EXPLAIN PIPELINE SELECT * FROM bnl_pipeline_l l LEFT JOIN bnl_pipeline_r r ON l.x < r.y)
WHERE explain LIKE '%BlockNestedLoopProbe × 3%';
SELECT 'delayed', count() FROM (EXPLAIN PIPELINE SELECT * FROM bnl_pipeline_l l LEFT JOIN bnl_pipeline_r r ON l.x < r.y)
WHERE explain LIKE '%DelayedPorts%';
SELECT 'totals', count() FROM (EXPLAIN PIPELINE SELECT * FROM bnl_pipeline_l l LEFT JOIN bnl_pipeline_r r ON l.x < r.y)
WHERE explain LIKE '%BlockNestedLoopTotals%';

-- `WITH TOTALS` on the probe side adds the transform that joins the two totals rows.
SELECT 'probe totals', count() FROM (
    EXPLAIN PIPELINE SELECT * FROM (SELECT id, sum(x) AS x FROM bnl_pipeline_l GROUP BY id WITH TOTALS) l
    LEFT JOIN bnl_pipeline_r r ON l.x < r.y)
WHERE explain LIKE '%BlockNestedLoopTotals%';

-- Build-side totals are stored by a single build stream, the one that owns the totals port.
SELECT 'build totals', count() FROM (
    EXPLAIN PIPELINE SELECT * FROM bnl_pipeline_l l
    LEFT JOIN (SELECT id, sum(y) AS y FROM bnl_pipeline_r GROUP BY id WITH TOTALS) r ON l.x < r.y)
WHERE explain LIKE '%BlockNestedLoopBuild%';
SELECT 'build totals streams', count() FROM (
    EXPLAIN PIPELINE SELECT * FROM bnl_pipeline_l l
    LEFT JOIN (SELECT id, sum(y) AS y FROM bnl_pipeline_r GROUP BY id WITH TOTALS) r ON l.x < r.y)
WHERE explain LIKE '%BlockNestedLoopBuild × %';
SELECT 'build totals joined', count() FROM (
    EXPLAIN PIPELINE SELECT * FROM bnl_pipeline_l l
    LEFT JOIN (SELECT id, sum(y) AS y FROM bnl_pipeline_r GROUP BY id WITH TOTALS) r ON l.x < r.y)
WHERE explain LIKE '%BlockNestedLoopTotals%';

-- The joined totals row itself, with no probe row to match: the same row a hash join produces for
-- the equivalent equi join. A side without totals of its own contributes its columns' defaults.
SELECT * FROM (SELECT id, x FROM bnl_pipeline_l WHERE 0) l
LEFT JOIN (SELECT id, sum(y) AS y FROM bnl_pipeline_r GROUP BY id WITH TOTALS) r ON l.x < r.y;
SELECT * FROM (SELECT id, x FROM bnl_pipeline_l WHERE 0) l
LEFT JOIN (SELECT id, sum(y) AS y FROM bnl_pipeline_r GROUP BY id WITH TOTALS) r ON l.id = r.id;
SELECT * FROM (SELECT id, sum(x) AS x FROM bnl_pipeline_l WHERE 0 GROUP BY id WITH TOTALS) l
LEFT JOIN bnl_pipeline_r r ON l.x < r.y;
SELECT * FROM (SELECT id, sum(x) AS x FROM bnl_pipeline_l WHERE 0 GROUP BY id WITH TOTALS) l
LEFT JOIN (SELECT id, sum(y) AS y FROM bnl_pipeline_r GROUP BY id WITH TOTALS) r ON l.x < r.y;

-- `EXPLAIN actions = 1` describes the operator: the kind, the strictness and the whole ON condition.
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '')
FROM (EXPLAIN actions = 1 SELECT * FROM bnl_pipeline_l l LEFT ANTI JOIN bnl_pipeline_r r ON l.x < r.y)
WHERE explain LIKE '%Type: %' OR explain LIKE '%Strictness: %' OR explain LIKE '%Condition: %';

-- ⚠️ The matching itself lands in task 4; until then the operator refuses to run.
SELECT * FROM bnl_pipeline_l l LEFT JOIN bnl_pipeline_r r ON l.x < r.y; -- { serverError NOT_IMPLEMENTED }

DROP TABLE bnl_pipeline_l;
DROP TABLE bnl_pipeline_r;
