-- Random settings limits: optimize_read_in_order=(1, None); optimize_distinct_in_order=(1, None); optimize_use_projections=(1, None)

-- A table read through `merge` or a `Merge` table can be served from a normal projection with another ORDER BY.
-- Its rows must not be taken as sorted by the table's sorting key.

DROP TABLE IF EXISTS t_proj;
DROP TABLE IF EXISTS t_plain;
DROP TABLE IF EXISTS t_partial;
DROP TABLE IF EXISTS t_merge;

CREATE TABLE t_proj (a Int32, b Int32, PROJECTION p (SELECT a, b ORDER BY b)) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_proj SELECT number % 10, number FROM numbers(100000);
OPTIMIZE TABLE t_proj FINAL;

SELECT groupArray(a) FROM (SELECT a FROM merge(currentDatabase(), '^t_proj$') WHERE b < 5000 ORDER BY a LIMIT 12);
SELECT count() FROM (SELECT a FROM merge(currentDatabase(), '^t_proj$') WHERE b < 5000 ORDER BY a LIMIT 1 BY a);
SELECT groupArray(a) FROM (SELECT a FROM merge(currentDatabase(), '^t_proj$') PREWHERE b < 5000 ORDER BY a LIMIT 12);

CREATE TABLE t_merge AS t_proj ENGINE = Merge(currentDatabase(), '^t_proj$');
SELECT groupArray(a) FROM (SELECT a FROM t_merge WHERE b < 5000 ORDER BY a LIMIT 12);

-- Only the second part has the projection.
CREATE TABLE t_partial (a Int32, b Int32) ENGINE = MergeTree ORDER BY a;
SYSTEM STOP MERGES t_partial;
INSERT INTO t_partial SELECT 5 + number % 5, 1000000 + number FROM numbers(50000);
ALTER TABLE t_partial ADD PROJECTION p (SELECT a, b ORDER BY b);
INSERT INTO t_partial SELECT number % 10, number FROM numbers(100000);
SELECT groupArray(a) FROM (SELECT a FROM merge(currentDatabase(), '^t_partial$') WHERE b < 5000 OR b >= 1000000 ORDER BY a LIMIT 12);

CREATE TABLE t_plain (a Int32, b Int32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_plain SELECT 5 + number % 5, number FROM numbers(100000);
SELECT groupArray(a) FROM (SELECT a FROM merge(currentDatabase(), '^t_(proj|plain)$') WHERE b < 5000 ORDER BY a LIMIT 12);

SELECT count() FROM (SELECT DISTINCT a FROM merge(currentDatabase(), '^t_proj$') WHERE b < 5000);

-- Without the projection the table is still read in order.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1 SELECT a FROM merge(currentDatabase(), '^t_proj$') WHERE b < 5000 ORDER BY a LIMIT 12
    SETTINGS optimize_use_projections = 0, explain_query_plan_default = 'pretty'
) WHERE explain ILIKE '%Read type: InOrder%';

DROP TABLE t_merge;
DROP TABLE t_plain;
DROP TABLE t_partial;
DROP TABLE t_proj;
