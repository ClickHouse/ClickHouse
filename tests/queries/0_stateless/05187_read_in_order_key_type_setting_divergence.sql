-- A key expression is resolved twice: once by the table under its own settings, once by the
-- query under the session's. When a setting changes its result type, the two are different
-- functions with different orders, so read-in-order must not treat them as interchangeable.
-- Arms 1 to 5 pin query_plan_read_in_order = 1 and cover the plan-based matcher; arms 6 and 7 pin it
-- off under the old analyzer and cover the legacy one.

-- Arm 1: the ORDER BY divergence. Wrong row order, visible with no assert involved.
DROP TABLE IF EXISTS t_order;
SET cast_keep_nullable = 0;
CREATE TABLE t_order (json JSON) ENGINE = MergeTree ORDER BY CAST(json.b, 'String');
SYSTEM STOP MERGES t_order;
INSERT INTO t_order VALUES ('{"b":"a"}'), ('{"b":"c"}');
INSERT INTO t_order VALUES ('{"b":"b"}'), ('{"a":1}');
SET cast_keep_nullable = 1;
SELECT CAST(json.b, 'String') FROM t_order ORDER BY CAST(json.b, 'String')
SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1,
         read_in_order_use_virtual_row = 0, max_threads = 4;

-- Arm 2: the divergence hidden under an equal-typed parent. ifNull returns String in both
-- contexts, so only a check at every mapping rejects the inner CAST.
DROP TABLE IF EXISTS t_hidden;
SET cast_keep_nullable = 0;
CREATE TABLE t_hidden (json JSON) ENGINE = MergeTree ORDER BY ifNull(CAST(json.b, 'String'), 'zzz');
SYSTEM STOP MERGES t_hidden;
INSERT INTO t_hidden VALUES ('{"b":"a"}'), ('{"b":"c"}');
INSERT INTO t_hidden VALUES ('{"b":"b"}'), ('{"a":1}');
SET cast_keep_nullable = 1;
SELECT ifNull(CAST(json.b, 'String'), 'zzz') FROM t_hidden
ORDER BY ifNull(CAST(json.b, 'String'), 'zzz')
SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1,
         read_in_order_use_virtual_row = 0, max_threads = 4;

-- Arm 3: aggregation-in-order over the same divergence splits groups, so the counts are wrong.
DROP TABLE IF EXISTS t_agg;
SET cast_keep_nullable = 0;
CREATE TABLE t_agg (json JSON) ENGINE = MergeTree ORDER BY CAST(json.b, 'String');
SYSTEM STOP MERGES t_agg;
INSERT INTO t_agg VALUES ('{"b":"a"}'), ('{"a":1}');
INSERT INTO t_agg VALUES ('{"b":"a"}'), ('{"b":"b"}');
INSERT INTO t_agg VALUES ('{"a":2}'), ('{"b":"b"}');
SET cast_keep_nullable = 1;
SELECT CAST(json.b, 'String') AS v, count() AS n FROM t_agg GROUP BY v ORDER BY v
SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1,
         query_plan_read_in_order = 1, read_in_order_use_virtual_row = 0, max_threads = 4;

-- Arm 4: the reported failure. The virtual row was built with the query type and compared
-- against the storage type, aborting in a debug build and throwing otherwise.
SELECT CAST(json.b, 'String') FROM t_order ORDER BY CAST(json.b, 'String')
SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1,
         read_in_order_use_virtual_row = 1, max_threads = 4,
         read_in_order_two_level_merge_threshold = 0;

-- Arm 5: control. With the types in agreement the optimization is still applied, so the
-- rejection is narrow rather than a blanket disable.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT CAST(json.b, 'String') FROM t_order ORDER BY CAST(json.b, 'String')
    SETTINGS cast_keep_nullable = 0, optimize_read_in_order = 1,
             query_plan_read_in_order = 1, read_in_order_use_virtual_row = 0, max_threads = 4
) WHERE explain ILIKE '%Read type: InOrder%';

-- Arm 6: the legacy matcher, reached with the plan optimization off under the old analyzer. It
-- compares the key by name alone, so the same divergence advertises an order the parts do not have.
SET enable_analyzer = 0;
SELECT CAST(json.b, 'String') FROM t_order ORDER BY CAST(json.b, 'String')
SETTINGS query_plan_read_in_order = 0, optimize_read_in_order = 1,
         read_in_order_use_virtual_row = 0, max_threads = 4;

-- Arm 7: control for arm 6. With the types in agreement the legacy matcher still reads in order.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT CAST(json.b, 'String') FROM t_order ORDER BY CAST(json.b, 'String')
    SETTINGS cast_keep_nullable = 0, query_plan_read_in_order = 0, optimize_read_in_order = 1,
             read_in_order_use_virtual_row = 0, max_threads = 4
) WHERE explain ILIKE '%Read type: InOrder%';

DROP TABLE t_order;
DROP TABLE t_hidden;
DROP TABLE t_agg;
