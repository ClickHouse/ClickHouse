-- Tags: no-parallel-replicas
-- no-parallel-replicas: the aggregate projection optimization is declined under parallel
-- reading, so the EXPLAIN projection-selection assertions below would not hold.

-- Every result is asserted as equal to the optimize_use_projections = 0 result, so the assertion is
-- the value set, not an ordering that SELECT DISTINCT ... ORDER BY <column outside the DISTINCT list>
-- leaves ambiguous. Each shape that must keep using the projection also asserts its selection, so a
-- silent fallback to the ordinary read cannot pass as a correct result.

DROP TABLE IF EXISTS t_distinct_proj;

CREATE TABLE t_distinct_proj
(
    v Int64,
    w Int64,
    PROJECTION p (SELECT v, count() GROUP BY v)
)
ENGINE = MergeTree ORDER BY v;

INSERT INTO t_distinct_proj SELECT number, number % 3 FROM numbers(10);

-- ORDER BY a column the DISTINCT output does not carry.
SELECT
    (SELECT arraySort(groupArray(x)) FROM (SELECT DISTINCT v * 2 AS x FROM t_distinct_proj ORDER BY v SETTINGS optimize_use_projections = 1))
  = (SELECT arraySort(groupArray(x)) FROM (SELECT DISTINCT v * 2 AS x FROM t_distinct_proj ORDER BY v SETTINGS optimize_use_projections = 0));

-- Same, with a filter.
SELECT
    (SELECT arraySort(groupArray(x)) FROM (SELECT DISTINCT v * 2 AS x FROM t_distinct_proj WHERE v > 2 ORDER BY v SETTINGS optimize_use_projections = 1))
  = (SELECT arraySort(groupArray(x)) FROM (SELECT DISTINCT v * 2 AS x FROM t_distinct_proj WHERE v > 2 ORDER BY v SETTINGS optimize_use_projections = 0));

-- ORDER BY a column the projection cannot produce: the projection must be declined, not fail.
SELECT
    (SELECT arraySort(groupArray(x)) FROM (SELECT DISTINCT v AS x FROM t_distinct_proj ORDER BY w SETTINGS optimize_use_projections = 1))
  = (SELECT arraySort(groupArray(x)) FROM (SELECT DISTINCT v AS x FROM t_distinct_proj ORDER BY w SETTINGS optimize_use_projections = 0));

-- Repeated name in the DISTINCT list: the header carries it twice and is matched by position.
SELECT
    (SELECT arraySort(groupArray((a, b))) FROM (SELECT DISTINCT v * 2 AS a, v * 2 AS b FROM t_distinct_proj ORDER BY v SETTINGS optimize_use_projections = 1))
  = (SELECT arraySort(groupArray((a, b))) FROM (SELECT DISTINCT v * 2 AS a, v * 2 AS b FROM t_distinct_proj ORDER BY v SETTINGS optimize_use_projections = 0));

-- Shapes where the header already equalled the DISTINCT list.
SELECT
    (SELECT arraySort(groupArray(x)) FROM (SELECT DISTINCT v AS x FROM t_distinct_proj ORDER BY v SETTINGS optimize_use_projections = 1))
  = (SELECT arraySort(groupArray(x)) FROM (SELECT DISTINCT v AS x FROM t_distinct_proj ORDER BY v SETTINGS optimize_use_projections = 0));
SELECT
    (SELECT arraySort(groupArray(x)) FROM (SELECT DISTINCT v * 2 AS x FROM t_distinct_proj ORDER BY v * 2 SETTINGS optimize_use_projections = 1))
  = (SELECT arraySort(groupArray(x)) FROM (SELECT DISTINCT v * 2 AS x FROM t_distinct_proj ORDER BY v * 2 SETTINGS optimize_use_projections = 0));
SELECT
    (SELECT arraySort(groupArray((a, b))) FROM (SELECT DISTINCT v * 2 AS a, v * 2 AS b FROM t_distinct_proj SETTINGS optimize_use_projections = 1))
  = (SELECT arraySort(groupArray((a, b))) FROM (SELECT DISTINCT v * 2 AS a, v * 2 AS b FROM t_distinct_proj SETTINGS optimize_use_projections = 0));

-- The projection is still selected where it can produce the whole header, and declined where it cannot.
SELECT count() > 0 FROM (EXPLAIN SELECT DISTINCT v * 2 FROM t_distinct_proj ORDER BY v SETTINGS optimize_use_projections = 1) WHERE explain ILIKE '%ReadFromMergeTree (p)%';
SELECT count() > 0 FROM (EXPLAIN SELECT DISTINCT v FROM t_distinct_proj ORDER BY w SETTINGS optimize_use_projections = 1) WHERE explain ILIKE '%ReadFromMergeTree (p)%';
SELECT count() > 0 FROM (EXPLAIN SELECT DISTINCT v * 2 FROM t_distinct_proj WHERE v > 2 ORDER BY v SETTINGS optimize_use_projections = 1) WHERE explain ILIKE '%ReadFromMergeTree (p)%';
SELECT count() > 0 FROM (EXPLAIN SELECT DISTINCT v * 2 AS a, v * 2 AS b FROM t_distinct_proj ORDER BY v SETTINGS optimize_use_projections = 1) WHERE explain ILIKE '%ReadFromMergeTree (p)%';

DROP TABLE t_distinct_proj;

-- The ORDER BY column is itself a projection key.
DROP TABLE IF EXISTS t_distinct_proj_two_keys;

CREATE TABLE t_distinct_proj_two_keys
(
    v Int64,
    w Int64,
    PROJECTION p (SELECT v, w, count() GROUP BY v, w)
)
ENGINE = MergeTree ORDER BY v;

INSERT INTO t_distinct_proj_two_keys SELECT number, number % 3 FROM numbers(10);

SELECT
    (SELECT arraySort(groupArray(x)) FROM (SELECT DISTINCT v * 2 AS x FROM t_distinct_proj_two_keys ORDER BY w SETTINGS optimize_use_projections = 1))
  = (SELECT arraySort(groupArray(x)) FROM (SELECT DISTINCT v * 2 AS x FROM t_distinct_proj_two_keys ORDER BY w SETTINGS optimize_use_projections = 0));

SELECT count() > 0 FROM (EXPLAIN SELECT DISTINCT v * 2 FROM t_distinct_proj_two_keys ORDER BY w SETTINGS optimize_use_projections = 1) WHERE explain ILIKE '%ReadFromMergeTree (p)%';

DROP TABLE t_distinct_proj_two_keys;

-- Only some parts carry projection data, so DistinctStep reads through a union of both sources.
DROP TABLE IF EXISTS t_distinct_proj_partial;

CREATE TABLE t_distinct_proj_partial (a Int64) ENGINE = MergeTree ORDER BY a;

-- A merge would materialize projection data for the older part and collapse the union.
SYSTEM STOP MERGES t_distinct_proj_partial;

INSERT INTO t_distinct_proj_partial SELECT number FROM numbers(5);
ALTER TABLE t_distinct_proj_partial ADD PROJECTION p (SELECT a, count() GROUP BY a);
INSERT INTO t_distinct_proj_partial SELECT number FROM numbers(5, 5);

SELECT
    (SELECT arraySort(groupArray(x)) FROM (SELECT DISTINCT a * 2 AS x FROM t_distinct_proj_partial ORDER BY a SETTINGS optimize_use_projections = 1))
  = (SELECT arraySort(groupArray(x)) FROM (SELECT DISTINCT a * 2 AS x FROM t_distinct_proj_partial ORDER BY a SETTINGS optimize_use_projections = 0));

SELECT countIf(explain ILIKE '%ReadFromMergeTree (p)%') > 0
   AND countIf(explain ILIKE '%ReadFromMergeTree (%t_distinct_proj_partial)%') > 0
FROM (EXPLAIN SELECT DISTINCT a * 2 FROM t_distinct_proj_partial ORDER BY a SETTINGS optimize_use_projections = 1);

DROP TABLE t_distinct_proj_partial;

-- LowCardinality ORDER BY column: the splice converts types rather than dropping the column.
DROP TABLE IF EXISTS t_distinct_proj_lc;

CREATE TABLE t_distinct_proj_lc
(
    s LowCardinality(String),
    PROJECTION p (SELECT s, count() GROUP BY s)
)
ENGINE = MergeTree ORDER BY s;

INSERT INTO t_distinct_proj_lc SELECT toString(number) FROM numbers(5);

SELECT
    (SELECT arraySort(groupArray(x)) FROM (SELECT DISTINCT concat(s, 'x') AS x FROM t_distinct_proj_lc ORDER BY s SETTINGS optimize_use_projections = 1))
  = (SELECT arraySort(groupArray(x)) FROM (SELECT DISTINCT concat(s, 'x') AS x FROM t_distinct_proj_lc ORDER BY s SETTINGS optimize_use_projections = 0));

SELECT count() > 0 FROM (EXPLAIN SELECT DISTINCT concat(s, 'x') FROM t_distinct_proj_lc ORDER BY s SETTINGS optimize_use_projections = 1) WHERE explain ILIKE '%ReadFromMergeTree (p)%';

DROP TABLE t_distinct_proj_lc;

-- Nullable ORDER BY column, including a NULL. groupArray drops NULLs, so the compared value wraps
-- each row in a tuple to keep the NULL row observable.
DROP TABLE IF EXISTS t_distinct_proj_nullable;

CREATE TABLE t_distinct_proj_nullable
(
    n Nullable(Int64),
    PROJECTION p (SELECT n, count() GROUP BY n)
)
ENGINE = MergeTree ORDER BY n SETTINGS allow_nullable_key = 1;

INSERT INTO t_distinct_proj_nullable SELECT number FROM numbers(5);
INSERT INTO t_distinct_proj_nullable VALUES (NULL);

SELECT
    (SELECT arraySort(groupArray(tuple(isNull(x), x))) FROM (SELECT DISTINCT n * 2 AS x FROM t_distinct_proj_nullable ORDER BY n SETTINGS optimize_use_projections = 1))
  = (SELECT arraySort(groupArray(tuple(isNull(x), x))) FROM (SELECT DISTINCT n * 2 AS x FROM t_distinct_proj_nullable ORDER BY n SETTINGS optimize_use_projections = 0));

-- The NULL row survives the comparison: 5 values plus the NULL.
SELECT length(arraySort(groupArray(tuple(isNull(x), x)))) FROM (SELECT DISTINCT n * 2 AS x FROM t_distinct_proj_nullable ORDER BY n SETTINGS optimize_use_projections = 1);

SELECT count() > 0 FROM (EXPLAIN SELECT DISTINCT n * 2 FROM t_distinct_proj_nullable ORDER BY n SETTINGS optimize_use_projections = 1) WHERE explain ILIKE '%ReadFromMergeTree (p)%';

DROP TABLE t_distinct_proj_nullable;
