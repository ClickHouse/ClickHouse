-- Fuse a filter on ARRAY JOINed element columns into the ARRAY JOIN step. Must never change results,
-- so each case compares query_plan_fuse_filter_into_array_join on vs off and expects equality (1).
SET enable_analyzer = 1;
-- fusion is skipped for serialized plans, pin it so the plan-shape checks hold in the distributed-plan suite
SET serialize_query_plan = 0;

DROP TABLE IF EXISTS t_fuse;
CREATE TABLE t_fuse (id UInt64, arr Array(String), payload String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_fuse SELECT number, arrayMap(x -> concat('e', toString(x % 5)), range(number % 7)), repeat('P', 64) FROM numbers(300);

-- inner, element-only predicate
SELECT (SELECT sum(cityHash64(elem, payload)) FROM (SELECT elem, payload FROM t_fuse ARRAY JOIN arr AS elem WHERE elem = 'e2') SETTINGS query_plan_fuse_filter_into_array_join = 1)
     = (SELECT sum(cityHash64(elem, payload)) FROM (SELECT elem, payload FROM t_fuse ARRAY JOIN arr AS elem WHERE elem = 'e2') SETTINGS query_plan_fuse_filter_into_array_join = 0);

-- element predicate ANDed with a passenger predicate (only the element part fuses)
SELECT (SELECT count() FROM (SELECT elem FROM t_fuse ARRAY JOIN arr AS elem WHERE elem != 'e0' AND payload != '') SETTINGS query_plan_fuse_filter_into_array_join = 1)
     = (SELECT count() FROM (SELECT elem FROM t_fuse ARRAY JOIN arr AS elem WHERE elem != 'e0' AND payload != '') SETTINGS query_plan_fuse_filter_into_array_join = 0);

-- short-circuit inside the element predicate
SELECT (SELECT count() FROM (SELECT elem FROM t_fuse ARRAY JOIN arr AS elem WHERE elem != 'e0' AND length(elem) = 2) SETTINGS query_plan_fuse_filter_into_array_join = 1)
     = (SELECT count() FROM (SELECT elem FROM t_fuse ARRAY JOIN arr AS elem WHERE elem != 'e0' AND length(elem) = 2) SETTINGS query_plan_fuse_filter_into_array_join = 0);

-- all elements filtered out
SELECT (SELECT count() FROM (SELECT elem FROM t_fuse ARRAY JOIN arr AS elem WHERE elem = 'nomatch') SETTINGS query_plan_fuse_filter_into_array_join = 1)
     = (SELECT count() FROM (SELECT elem FROM t_fuse ARRAY JOIN arr AS elem WHERE elem = 'nomatch') SETTINGS query_plan_fuse_filter_into_array_join = 0);

-- LEFT ARRAY JOIN: empty arrays emit a default element, which the fused filter must treat like the filter above
SELECT (SELECT sum(cityHash64(id, elem)) FROM (SELECT id, elem FROM t_fuse LEFT ARRAY JOIN arr AS elem WHERE elem = 'e1' OR elem = '') SETTINGS query_plan_fuse_filter_into_array_join = 1)
     = (SELECT sum(cityHash64(id, elem)) FROM (SELECT id, elem FROM t_fuse LEFT ARRAY JOIN arr AS elem WHERE elem = 'e1' OR elem = '') SETTINGS query_plan_fuse_filter_into_array_join = 0);

-- several aligned arrays, predicate on one
DROP TABLE IF EXISTS t_fuse2;
CREATE TABLE t_fuse2 (id UInt64, a Array(UInt32), b Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_fuse2 SELECT number, arrayMap(x -> toUInt32(x % 4), range(number % 5)), arrayMap(x -> concat('b', toString(x)), range(number % 5)) FROM numbers(200);
SELECT (SELECT sum(cityHash64(x, y)) FROM (SELECT x, y FROM t_fuse2 ARRAY JOIN a AS x, b AS y WHERE x > 1) SETTINGS query_plan_fuse_filter_into_array_join = 1)
     = (SELECT sum(cityHash64(x, y)) FROM (SELECT x, y FROM t_fuse2 ARRAY JOIN a AS x, b AS y WHERE x > 1) SETTINGS query_plan_fuse_filter_into_array_join = 0);

-- Map ARRAY JOIN, predicate on the key
DROP TABLE IF EXISTS t_fuse3;
CREATE TABLE t_fuse3 (id UInt64, m Map(String, UInt32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_fuse3 SELECT number, map('k' || toString(number % 3), toUInt32(number), 'z', toUInt32(number + 1)) FROM numbers(100);
SELECT (SELECT sum(cityHash64(k, v)) FROM (SELECT m.1 AS k, m.2 AS v FROM t_fuse3 ARRAY JOIN m WHERE m.1 = 'z') SETTINGS query_plan_fuse_filter_into_array_join = 1)
     = (SELECT sum(cityHash64(k, v)) FROM (SELECT m.1 AS k, m.2 AS v FROM t_fuse3 ARRAY JOIN m WHERE m.1 = 'z') SETTINGS query_plan_fuse_filter_into_array_join = 0);

-- Nullable elements
DROP TABLE IF EXISTS t_fuse4;
CREATE TABLE t_fuse4 (id UInt64, arr Array(Nullable(Int32))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_fuse4 SELECT number, arrayMap(x -> if(x % 2 = 0, toNullable(toInt32(x)), NULL), range(number % 6)) FROM numbers(150);
SELECT (SELECT sum(cityHash64(assumeNotNull(elem))) FROM (SELECT elem FROM t_fuse4 ARRAY JOIN arr AS elem WHERE elem IS NOT NULL AND elem > 1) SETTINGS query_plan_fuse_filter_into_array_join = 1)
     = (SELECT sum(cityHash64(assumeNotNull(elem))) FROM (SELECT elem FROM t_fuse4 ARRAY JOIN arr AS elem WHERE elem IS NOT NULL AND elem > 1) SETTINGS query_plan_fuse_filter_into_array_join = 0);

-- Fusion is skipped under plan serialization, the result is still correct.
SELECT (SELECT sum(cityHash64(elem, payload)) FROM (SELECT elem, payload FROM t_fuse ARRAY JOIN arr AS elem WHERE elem = 'e2') SETTINGS query_plan_fuse_filter_into_array_join = 1, serialize_query_plan = 1)
     = (SELECT sum(cityHash64(elem, payload)) FROM (SELECT elem, payload FROM t_fuse ARRAY JOIN arr AS elem WHERE elem = 'e2') SETTINGS query_plan_fuse_filter_into_array_join = 0);

-- The fused plan carries an element filter on the ArrayJoin step, and only with the setting on.
SELECT countIf(explain LIKE '%Element filter column%') > 0 FROM (EXPLAIN actions = 1 SELECT elem FROM t_fuse ARRAY JOIN arr AS elem WHERE elem = 'e2' SETTINGS query_plan_fuse_filter_into_array_join = 1);
SELECT countIf(explain LIKE '%Element filter column%') FROM (EXPLAIN actions = 1 SELECT elem FROM t_fuse ARRAY JOIN arr AS elem WHERE elem = 'e2' SETTINGS query_plan_fuse_filter_into_array_join = 0);

-- Regression: the WHERE expression is projected and its non-element part folds to a constant; a sibling
-- conjunct stays above so the pass must not fuse (feeding a UNION). Result must match unfused.
DROP TABLE IF EXISTS t_fuse5;
CREATE TABLE t_fuse5 (id UInt64, arr Array(Int64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_fuse5 SELECT number, [number, number + 1, 0] FROM numbers(20);
SELECT
    (SELECT groupArray((c, w)) FROM (SELECT elem AS c, (elem != 0 AND NULL) AS w FROM t_fuse5 ARRAY JOIN arr AS elem WHERE (elem != 0 AND NULL) UNION ALL SELECT id AS c, (id > 100) AS w FROM t_fuse5 ORDER BY c, w) SETTINGS query_plan_fuse_filter_into_array_join = 1)
  = (SELECT groupArray((c, w)) FROM (SELECT elem AS c, (elem != 0 AND NULL) AS w FROM t_fuse5 ARRAY JOIN arr AS elem WHERE (elem != 0 AND NULL) UNION ALL SELECT id AS c, (id > 100) AS w FROM t_fuse5 ORDER BY c, w) SETTINGS query_plan_fuse_filter_into_array_join = 0);

-- Regression: a throwing element predicate ANDed with a non-fusible sibling. Fusing intDiv(1, elem) out
-- would evaluate it on elem = 0 where the sibling short-circuits it away, so the pass must not fuse.
DROP TABLE IF EXISTS t_fuse6;
CREATE TABLE t_fuse6 (arr Array(Int64), payload String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_fuse6 VALUES ([0, 1, 2], 'p');
SELECT
    (SELECT groupArray(elem) FROM (SELECT elem FROM t_fuse6 ARRAY JOIN arr AS elem WHERE (elem != 0 OR payload = 'zzz') AND (intDiv(1, elem) > 0) ORDER BY elem) SETTINGS query_plan_fuse_filter_into_array_join = 1)
  = (SELECT groupArray(elem) FROM (SELECT elem FROM t_fuse6 ARRAY JOIN arr AS elem WHERE (elem != 0 OR payload = 'zzz') AND (intDiv(1, elem) > 0) ORDER BY elem) SETTINGS query_plan_fuse_filter_into_array_join = 0);

-- Regression: with short-circuit off a FilterStep masks a throwing atom by splitting the AND, which the
-- fused single evaluation can't reproduce, so a multi-atom AND must not fuse in that mode. Assert the bail
-- via EXPLAIN (no element filter) rather than by executing, whose masking also depends on query_plan_merge_filters.
SELECT countIf(explain LIKE '%Element filter%') = 0 FROM (EXPLAIN actions = 1 SELECT elem FROM t_fuse6 ARRAY JOIN arr AS elem WHERE elem != 0 AND intDiv(1, elem) > 0 SETTINGS query_plan_fuse_filter_into_array_join = 1, short_circuit_function_evaluation = 'disable', serialize_query_plan = 0);
-- but with short-circuit on the same multi-atom AND still fuses
SELECT countIf(explain LIKE '%Element filter%') > 0 FROM (EXPLAIN actions = 1 SELECT elem FROM t_fuse6 ARRAY JOIN arr AS elem WHERE elem != 0 AND intDiv(1, elem) > 0 SETTINGS query_plan_fuse_filter_into_array_join = 1, short_circuit_function_evaluation = 'enable', serialize_query_plan = 0);

DROP TABLE t_fuse;
DROP TABLE t_fuse2;
DROP TABLE t_fuse3;
DROP TABLE t_fuse4;
DROP TABLE t_fuse5;
DROP TABLE t_fuse6;
