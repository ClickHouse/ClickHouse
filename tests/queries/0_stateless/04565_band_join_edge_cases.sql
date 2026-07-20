-- Tags: no-old-analyzer

-- Band join edge cases: empty sides, single rows, all-equal keys, empty and inverted
-- intervals, and the `BETWEEN`-style shared column on the interval side.

-- Keep the written join order: the band join detects only the point-side-on-the-left
-- orientation for now, so a planner swap would silently change the executed algorithm.
SET query_plan_optimize_join_order_limit = 0;
SET join_algorithm = 'band_join,hash';
SET cross_to_inner_join_rewrite = 0;

DROP TABLE IF EXISTS ec_p;
DROP TABLE IF EXISTS ec_i;

CREATE TABLE ec_p (id UInt32, t Int64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ec_i (id UInt32, lo Int64, hi Int64) ENGINE = MergeTree ORDER BY id;

SELECT 'both empty', count() FROM ec_p p JOIN ec_i i ON p.t >= i.lo AND p.t <= i.hi;

INSERT INTO ec_p SELECT number, number % 10 FROM numbers(100);
SELECT 'empty intervals', count() FROM ec_p p JOIN ec_i i ON p.t >= i.lo AND p.t <= i.hi;

TRUNCATE TABLE ec_p;
INSERT INTO ec_i SELECT number, number % 8, number % 8 + 2 FROM numbers(100);
SELECT 'empty points', count() FROM ec_p p JOIN ec_i i ON p.t >= i.lo AND p.t <= i.hi;

INSERT INTO ec_p SELECT number, number % 10 FROM numbers(100);
SELECT 'small dense',
    (SELECT arraySort(groupArray((p.id, i.id))) FROM ec_p p JOIN ec_i i ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id))) FROM ec_p p, ec_i i WHERE p.t >= i.lo AND p.t <= i.hi) AS oracle_ok,
    (SELECT count() FROM ec_p p JOIN ec_i i ON p.t >= i.lo AND p.t <= i.hi) AS cnt;

-- One row on each side, matching and non-matching
SELECT 'single row match', count() FROM (SELECT 5 :: Int64 AS t) p JOIN (SELECT 1 :: Int64 AS lo, 9 :: Int64 AS hi) i ON p.t >= i.lo AND p.t <= i.hi;
SELECT 'single row miss', count() FROM (SELECT 15 :: Int64 AS t) p JOIN (SELECT 1 :: Int64 AS lo, 9 :: Int64 AS hi) i ON p.t >= i.lo AND p.t <= i.hi;

-- Bound touching: loose brackets include the endpoints, strict ones exclude them
SELECT 'loose endpoints', count() FROM (SELECT 1 :: Int64 AS t UNION ALL SELECT 9) p JOIN (SELECT 1 :: Int64 AS lo, 9 :: Int64 AS hi) i ON p.t >= i.lo AND p.t <= i.hi;
SELECT 'strict endpoints', count() FROM (SELECT 1 :: Int64 AS t UNION ALL SELECT 9) p JOIN (SELECT 1 :: Int64 AS lo, 9 :: Int64 AS hi) i ON p.t > i.lo AND p.t < i.hi;

-- Every key equal on both sides: full cross product under loose brackets, empty under strict
SELECT 'all equal loose', count() FROM (SELECT 7 :: Int64 AS t FROM numbers(50)) p JOIN (SELECT 7 :: Int64 AS lo, 7 :: Int64 AS hi FROM numbers(50)) i ON p.t >= i.lo AND p.t <= i.hi;
SELECT 'all equal strict', count() FROM (SELECT 7 :: Int64 AS t FROM numbers(50)) p JOIN (SELECT 7 :: Int64 AS lo, 7 :: Int64 AS hi FROM numbers(50)) i ON p.t > i.lo AND p.t < i.hi;

-- Inverted intervals (hi < lo) match nothing; the aliases must not shadow the source
-- columns, or `ihi` would capture the shifted `ilo`
SELECT 'inverted intervals', count() FROM ec_p p JOIN (SELECT id, lo + 5 AS ilo, lo AS ihi FROM ec_i) i ON p.t >= i.ilo AND p.t <= i.ihi;

-- The same interval column serves as both bounds (a point-in-point band)
SELECT 'shared interval column',
    (SELECT arraySort(groupArray((p.id, i.id))) FROM ec_p p JOIN ec_i i ON p.t >= i.lo AND p.t <= i.lo)
        = (SELECT arraySort(groupArray((p.id, i.id))) FROM ec_p p, ec_i i WHERE p.t >= i.lo AND p.t <= i.lo) AS oracle_ok,
    (SELECT count() FROM ec_p p JOIN ec_i i ON p.t >= i.lo AND p.t <= i.lo) AS cnt;

-- Extra usable inequalities beyond the chosen pair stay as a filter over the result
SELECT 'extra inequality',
    (SELECT arraySort(groupArray((p.id, i.id))) FROM ec_p p JOIN ec_i i ON p.t >= i.lo AND p.t <= i.hi AND p.id < i.id)
        = (SELECT arraySort(groupArray((p.id, i.id))) FROM ec_p p, ec_i i WHERE p.t >= i.lo AND p.t <= i.hi AND p.id < i.id) AS oracle_ok,
    (SELECT count() FROM ec_p p JOIN ec_i i ON p.t >= i.lo AND p.t <= i.hi AND p.id < i.id) AS cnt;

DROP TABLE ec_p;
DROP TABLE ec_i;
