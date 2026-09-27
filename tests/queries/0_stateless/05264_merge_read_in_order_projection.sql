-- Random settings limits: optimize_read_in_order=(1, None); optimize_distinct_in_order=(1, None); optimize_use_projections=(1, None)

-- A table read through `merge` or a `Merge` table can be served from a normal projection with another ORDER BY.
-- Its rows must not be taken as sorted by the table's sorting key.

DROP TABLE IF EXISTS t_proj;
DROP TABLE IF EXISTS t_plain;
DROP TABLE IF EXISTS t_partial;
DROP TABLE IF EXISTS t_merge;
DROP TABLE IF EXISTS t_agree;
DROP TABLE IF EXISTS t_desc;

CREATE TABLE t_proj (a Int32, b Int32, PROJECTION p (SELECT a, b ORDER BY b)) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_proj SELECT number % 10, number FROM numbers(100000);
OPTIMIZE TABLE t_proj FINAL;

-- Each query over one table is followed by its plan, which must read projection `p`
-- (`force_optimize_projection_name` fails the EXPLAIN otherwise) and not in order.
SELECT groupArray(a) FROM (SELECT a FROM merge(currentDatabase(), '^t_proj$') WHERE b < 5000 ORDER BY a LIMIT 12);
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT groupArray(a) FROM (SELECT a FROM merge(currentDatabase(), '^t_proj$') WHERE b < 5000 ORDER BY a LIMIT 12)
    SETTINGS force_optimize_projection_name = 'p', explain_query_plan_default = 'pretty') WHERE explain ILIKE '%Read type: Default%';
SELECT count() FROM (SELECT a FROM merge(currentDatabase(), '^t_proj$') WHERE b < 5000 ORDER BY a LIMIT 1 BY a);
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM (SELECT a FROM merge(currentDatabase(), '^t_proj$') WHERE b < 5000 ORDER BY a LIMIT 1 BY a)
    SETTINGS force_optimize_projection_name = 'p', explain_query_plan_default = 'pretty') WHERE explain ILIKE '%Read type: Default%';
SELECT groupArray(a) FROM (SELECT a FROM merge(currentDatabase(), '^t_proj$') PREWHERE b < 5000 ORDER BY a LIMIT 12);
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT groupArray(a) FROM (SELECT a FROM merge(currentDatabase(), '^t_proj$') PREWHERE b < 5000 ORDER BY a LIMIT 12)
    SETTINGS force_optimize_projection_name = 'p', explain_query_plan_default = 'pretty') WHERE explain ILIKE '%Read type: Default%';

CREATE TABLE t_merge AS t_proj ENGINE = Merge(currentDatabase(), '^t_proj$');
SELECT groupArray(a) FROM (SELECT a FROM t_merge WHERE b < 5000 ORDER BY a LIMIT 12);
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT groupArray(a) FROM (SELECT a FROM t_merge WHERE b < 5000 ORDER BY a LIMIT 12)
    SETTINGS force_optimize_projection_name = 'p', explain_query_plan_default = 'pretty') WHERE explain ILIKE '%Read type: Default%';

-- Only the second part has the projection.
CREATE TABLE t_partial (a Int32, b Int32) ENGINE = MergeTree ORDER BY a;
SYSTEM STOP MERGES t_partial;
INSERT INTO t_partial SELECT 5 + number % 5, 1000000 + number FROM numbers(50000);
ALTER TABLE t_partial ADD PROJECTION p (SELECT a, b ORDER BY b);
INSERT INTO t_partial SELECT number % 10, number FROM numbers(100000);
SELECT groupArray(a) FROM (SELECT a FROM merge(currentDatabase(), '^t_partial$') WHERE b < 5000 OR b >= 1000000 ORDER BY a LIMIT 12);
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT groupArray(a) FROM (SELECT a FROM merge(currentDatabase(), '^t_partial$') WHERE b < 5000 OR b >= 1000000 ORDER BY a LIMIT 12)
    SETTINGS force_optimize_projection_name = 'p', explain_query_plan_default = 'pretty') WHERE explain ILIKE '%Read type: Default%';

CREATE TABLE t_plain (a Int32, b Int32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_plain SELECT 5 + number % 5, number FROM numbers(100000);
SELECT groupArray(a) FROM (SELECT a FROM merge(currentDatabase(), '^t_(proj|plain)$') WHERE b < 5000 ORDER BY a LIMIT 12);

SELECT count() FROM (SELECT DISTINCT a FROM merge(currentDatabase(), '^t_proj$') WHERE b < 5000);
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM (SELECT DISTINCT a FROM merge(currentDatabase(), '^t_proj$') WHERE b < 5000)
    SETTINGS force_optimize_projection_name = 'p', explain_query_plan_default = 'pretty') WHERE explain ILIKE '%Read type: Default%';

-- Without the projection the table is still read in order.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1 SELECT a FROM merge(currentDatabase(), '^t_proj$') WHERE b < 5000 ORDER BY a LIMIT 12
    SETTINGS optimize_use_projections = 0, explain_query_plan_default = 'pretty'
) WHERE explain ILIKE '%Read type: InOrder%';

-- A projection whose ORDER BY starts with the table's sorting key is still read in order.
CREATE TABLE t_agree (a Int32, b Int32, PROJECTION p (SELECT a, b ORDER BY (a, b))) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_agree SELECT number % 10, number FROM numbers(1000);
SELECT count() > 0 FROM (
    EXPLAIN actions = 1 SELECT a FROM merge(currentDatabase(), '^t_agree$') WHERE b < 5000 ORDER BY a LIMIT 12
    SETTINGS force_optimize_projection = 1, explain_query_plan_default = 'pretty'
) WHERE explain ILIKE '%Read type: InOrder%';

-- A projection sorted ascending by the column that the table's key sorts descending is not read in order either.
CREATE TABLE t_desc (a Int32, b Int32, PROJECTION p (SELECT a, b ORDER BY (a, b))) ENGINE = MergeTree ORDER BY a DESC;
INSERT INTO t_desc SELECT number % 10, number FROM numbers(1000);
SELECT groupArray(a) FROM (SELECT a FROM merge(currentDatabase(), '^t_desc$') WHERE b < 5000 ORDER BY a DESC LIMIT 12) SETTINGS prefer_optimize_projection = 1;
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT groupArray(a) FROM (SELECT a FROM merge(currentDatabase(), '^t_desc$') WHERE b < 5000 ORDER BY a DESC LIMIT 12)
    SETTINGS prefer_optimize_projection = 1, force_optimize_projection_name = 'p', explain_query_plan_default = 'pretty') WHERE explain ILIKE '%Read type: Default%';

DROP TABLE t_desc;
DROP TABLE t_agree;
DROP TABLE t_merge;
DROP TABLE t_plain;
DROP TABLE t_partial;
DROP TABLE t_proj;
