-- Predicate inference into the opposite table of an outer join converted to inner, with join_use_nulls = 1.
-- A predicate written on one side is inferred on the other through the equi-join condition, which lets the
-- other table use its primary key. With join_use_nulls the converted join keeps the nullability it added to
-- the inner side, so the inferred predicate has to be the opposite key cast to that nullable type.

SET enable_analyzer = 1;
SET query_plan_convert_outer_join_to_inner_join = 1;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_infer_l;
DROP TABLE IF EXISTS t_infer_r;

-- Distinct key names make every `Condition:` line below say by itself which table it belongs to.
CREATE TABLE t_infer_l (id UInt64, lk UInt64, lv UInt64) ENGINE = MergeTree ORDER BY lk;
CREATE TABLE t_infer_r (id UInt64, rk UInt64, rv UInt64) ENGINE = MergeTree ORDER BY rk;

-- The two key ranges overlap in [50, 119] only, so 5000 left rows and 30 right rows have no match. A filter
-- pushed into the side an outer join does not preserve would drop them, which the result rows below see.
-- The probed key 80 is matched on both sides.
INSERT INTO t_infer_l SELECT number, number % 120, number FROM numbers(12000);
INSERT INTO t_infer_r SELECT number, 50 + number, number FROM numbers(100);

SELECT '-- LEFT JOIN, predicate on the right key only, join_use_nulls = 1: lk is inferred';
SELECT substring(explain, position(explain, 'Condition:')) FROM (
    EXPLAIN PLAN indexes = 1
    SELECT count() FROM t_infer_l AS l
    LEFT JOIN t_infer_r AS r ON l.lk = r.rk
    WHERE r.rk = 80
    SETTINGS join_use_nulls = 1
) WHERE explain LIKE '%Condition:%';

SELECT '-- the same at join_use_nulls = 0, which already inferred it';
SELECT substring(explain, position(explain, 'Condition:')) FROM (
    EXPLAIN PLAN indexes = 1
    SELECT count() FROM t_infer_l AS l
    LEFT JOIN t_infer_r AS r ON l.lk = r.rk
    WHERE r.rk = 80
    SETTINGS join_use_nulls = 0
) WHERE explain LIKE '%Condition:%';

SELECT '-- RIGHT JOIN, predicate on the left key only, join_use_nulls = 1: rk is inferred';
SELECT substring(explain, position(explain, 'Condition:')) FROM (
    EXPLAIN PLAN indexes = 1
    SELECT count() FROM t_infer_l AS l
    RIGHT JOIN t_infer_r AS r ON l.lk = r.rk
    WHERE l.lk = 80
    SETTINGS join_use_nulls = 1
) WHERE explain LIKE '%Condition:%';

-- A predicate on the right key alone admits a right-only row, whose left side is null, so the left not
-- matched rows may be dropped but the right ones may not: FULL becomes RIGHT. The join kind is read off the
-- plan next to each condition, because the conditions alone do not distinguish RIGHT from INNER.
SELECT '-- FULL JOIN converted to RIGHT, join_use_nulls = 1: lk is inferred, rk is filtered directly';
SELECT substring(explain, position(explain, 'Condition:')) FROM (
    EXPLAIN PLAN indexes = 1
    SELECT count() FROM t_infer_l AS l
    FULL JOIN t_infer_r AS r ON l.lk = r.rk
    WHERE r.rk = 80
    SETTINGS join_use_nulls = 1
) WHERE explain LIKE '%Condition:%';
SELECT trim(explain) FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_infer_l AS l
    FULL JOIN t_infer_r AS r ON l.lk = r.rk
    WHERE r.rk = 80
    SETTINGS join_use_nulls = 1
) WHERE trim(explain) LIKE 'Type:%';

SELECT '-- FULL JOIN converted to LEFT, join_use_nulls = 1: rk is inferred, lk is filtered directly';
SELECT substring(explain, position(explain, 'Condition:')) FROM (
    EXPLAIN PLAN indexes = 1
    SELECT count() FROM t_infer_l AS l
    FULL JOIN t_infer_r AS r ON l.lk = r.rk
    WHERE l.lk = 80
    SETTINGS join_use_nulls = 1
) WHERE explain LIKE '%Condition:%';
SELECT trim(explain) FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_infer_l AS l
    FULL JOIN t_infer_r AS r ON l.lk = r.rk
    WHERE l.lk = 80
    SETTINGS join_use_nulls = 1
) WHERE trim(explain) LIKE 'Type:%';

-- Null rejecting on both sides, so both kinds of not matched row may be dropped: FULL becomes INNER. This is
-- the only shape in which both keys are converted and reachable at once, so both casts are built in one pass.
SELECT '-- FULL JOIN converted to INNER, join_use_nulls = 1: each key is inferred next to its own predicate';
SELECT substring(explain, position(explain, 'Condition:')) FROM (
    EXPLAIN PLAN indexes = 1
    SELECT count() FROM t_infer_l AS l
    FULL JOIN t_infer_r AS r ON l.lk = r.rk
    WHERE l.lk = 80 AND r.rk = 80
    SETTINGS join_use_nulls = 1
) WHERE explain LIKE '%Condition:%';
SELECT trim(explain) FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_infer_l AS l
    FULL JOIN t_infer_r AS r ON l.lk = r.rk
    WHERE l.lk = 80 AND r.rk = 80
    SETTINGS join_use_nulls = 1
) WHERE trim(explain) LIKE 'Type:%';

SELECT '-- a predicate on a non-key column infers nothing, so lk stays unconstrained';
SELECT substring(explain, position(explain, 'Condition:')) FROM (
    EXPLAIN PLAN indexes = 1
    SELECT count() FROM t_infer_l AS l
    LEFT JOIN t_infer_r AS r ON l.lk = r.rk
    WHERE r.rv = 80
    SETTINGS join_use_nulls = 1
) WHERE explain LIKE '%Condition:%';

SELECT '-- a LEFT JOIN that is not converted keeps its not-matched rows, so nothing may be inferred';
SELECT substring(explain, position(explain, 'Condition:')) FROM (
    EXPLAIN PLAN indexes = 1
    SELECT count() FROM t_infer_l AS l
    LEFT JOIN t_infer_r AS r ON l.lk = r.rk
    WHERE r.rk IS NULL
    SETTINGS join_use_nulls = 1
) WHERE explain LIKE '%Condition:%';

SELECT '-- the join kind of that query, next to the one of the converted query above';
SELECT trim(explain) FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_infer_l AS l
    LEFT JOIN t_infer_r AS r ON l.lk = r.rk
    WHERE r.rk IS NULL
    SETTINGS join_use_nulls = 1
) WHERE trim(explain) LIKE 'Type:%';

SELECT trim(explain) FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_infer_l AS l
    LEFT JOIN t_infer_r AS r ON l.lk = r.rk
    WHERE r.rk = 80
    SETTINGS join_use_nulls = 1
) WHERE trim(explain) LIKE 'Type:%';

SELECT '-- results do not depend on join_use_nulls or on the conversion';
SELECT count(), sum(l.id), sum(r.id) FROM t_infer_l AS l
LEFT JOIN t_infer_r AS r ON l.lk = r.rk WHERE r.rk = 80
SETTINGS join_use_nulls = 1, query_plan_convert_outer_join_to_inner_join = 1;
SELECT count(), sum(l.id), sum(r.id) FROM t_infer_l AS l
LEFT JOIN t_infer_r AS r ON l.lk = r.rk WHERE r.rk = 80
SETTINGS join_use_nulls = 1, query_plan_convert_outer_join_to_inner_join = 0;
SELECT count(), sum(l.id), sum(r.id) FROM t_infer_l AS l
LEFT JOIN t_infer_r AS r ON l.lk = r.rk WHERE r.rk = 80
SETTINGS join_use_nulls = 0, query_plan_convert_outer_join_to_inner_join = 1;
SELECT count(), sum(l.id), sum(r.id) FROM t_infer_l AS l
LEFT JOIN t_infer_r AS r ON l.lk = r.rk WHERE r.rk = 80
SETTINGS join_use_nulls = 0, query_plan_convert_outer_join_to_inner_join = 0;

SELECT count(), sum(l.id), sum(r.id) FROM t_infer_l AS l
RIGHT JOIN t_infer_r AS r ON l.lk = r.rk WHERE l.lk = 80
SETTINGS join_use_nulls = 1, query_plan_convert_outer_join_to_inner_join = 1;
SELECT count(), sum(l.id), sum(r.id) FROM t_infer_l AS l
RIGHT JOIN t_infer_r AS r ON l.lk = r.rk WHERE l.lk = 80
SETTINGS join_use_nulls = 1, query_plan_convert_outer_join_to_inner_join = 0;
SELECT count(), sum(l.id), sum(r.id) FROM t_infer_l AS l
RIGHT JOIN t_infer_r AS r ON l.lk = r.rk WHERE l.lk = 80
SETTINGS join_use_nulls = 0, query_plan_convert_outer_join_to_inner_join = 0;

-- The not preserved side of a FULL join contributes rows whose columns are null under join_use_nulls = 1 and
-- default under 0, so `coalesce` is what makes the two settings comparable at all.
SELECT '-- FULL JOIN converted to RIGHT: unmatched rows survive every setting';
SELECT count(), sum(coalesce(l.id, 0)), sum(coalesce(r.id, 0)) FROM t_infer_l AS l
FULL JOIN t_infer_r AS r ON l.lk = r.rk WHERE r.rk = 80
SETTINGS join_use_nulls = 1, query_plan_convert_outer_join_to_inner_join = 1;
SELECT count(), sum(coalesce(l.id, 0)), sum(coalesce(r.id, 0)) FROM t_infer_l AS l
FULL JOIN t_infer_r AS r ON l.lk = r.rk WHERE r.rk = 80
SETTINGS join_use_nulls = 1, query_plan_convert_outer_join_to_inner_join = 0;
SELECT count(), sum(coalesce(l.id, 0)), sum(coalesce(r.id, 0)) FROM t_infer_l AS l
FULL JOIN t_infer_r AS r ON l.lk = r.rk WHERE r.rk = 80
SETTINGS join_use_nulls = 0, query_plan_convert_outer_join_to_inner_join = 1;
SELECT count(), sum(coalesce(l.id, 0)), sum(coalesce(r.id, 0)) FROM t_infer_l AS l
FULL JOIN t_infer_r AS r ON l.lk = r.rk WHERE r.rk = 80
SETTINGS join_use_nulls = 0, query_plan_convert_outer_join_to_inner_join = 0;

SELECT '-- FULL JOIN converted to LEFT';
SELECT count(), sum(coalesce(l.id, 0)), sum(coalesce(r.id, 0)) FROM t_infer_l AS l
FULL JOIN t_infer_r AS r ON l.lk = r.rk WHERE l.lk = 80
SETTINGS join_use_nulls = 1, query_plan_convert_outer_join_to_inner_join = 1;
SELECT count(), sum(coalesce(l.id, 0)), sum(coalesce(r.id, 0)) FROM t_infer_l AS l
FULL JOIN t_infer_r AS r ON l.lk = r.rk WHERE l.lk = 80
SETTINGS join_use_nulls = 1, query_plan_convert_outer_join_to_inner_join = 0;
SELECT count(), sum(coalesce(l.id, 0)), sum(coalesce(r.id, 0)) FROM t_infer_l AS l
FULL JOIN t_infer_r AS r ON l.lk = r.rk WHERE l.lk = 80
SETTINGS join_use_nulls = 0, query_plan_convert_outer_join_to_inner_join = 1;
SELECT count(), sum(coalesce(l.id, 0)), sum(coalesce(r.id, 0)) FROM t_infer_l AS l
FULL JOIN t_infer_r AS r ON l.lk = r.rk WHERE l.lk = 80
SETTINGS join_use_nulls = 0, query_plan_convert_outer_join_to_inner_join = 0;

SELECT '-- FULL JOIN converted to INNER';
SELECT count(), sum(coalesce(l.id, 0)), sum(coalesce(r.id, 0)) FROM t_infer_l AS l
FULL JOIN t_infer_r AS r ON l.lk = r.rk WHERE l.lk = 80 AND r.rk = 80
SETTINGS join_use_nulls = 1, query_plan_convert_outer_join_to_inner_join = 1;
SELECT count(), sum(coalesce(l.id, 0)), sum(coalesce(r.id, 0)) FROM t_infer_l AS l
FULL JOIN t_infer_r AS r ON l.lk = r.rk WHERE l.lk = 80 AND r.rk = 80
SETTINGS join_use_nulls = 1, query_plan_convert_outer_join_to_inner_join = 0;
SELECT count(), sum(coalesce(l.id, 0)), sum(coalesce(r.id, 0)) FROM t_infer_l AS l
FULL JOIN t_infer_r AS r ON l.lk = r.rk WHERE l.lk = 80 AND r.rk = 80
SETTINGS join_use_nulls = 0, query_plan_convert_outer_join_to_inner_join = 1;
SELECT count(), sum(coalesce(l.id, 0)), sum(coalesce(r.id, 0)) FROM t_infer_l AS l
FULL JOIN t_infer_r AS r ON l.lk = r.rk WHERE l.lk = 80 AND r.rk = 80
SETTINGS join_use_nulls = 0, query_plan_convert_outer_join_to_inner_join = 0;

SELECT '-- the left rows with no match, which a pushed down predicate would have dropped';
SELECT count() FROM t_infer_l AS l
LEFT JOIN t_infer_r AS r ON l.lk = r.rk WHERE r.rk IS NULL
SETTINGS join_use_nulls = 1;

DROP TABLE t_infer_l;
DROP TABLE t_infer_r;

-- A side is reported as type changing as a whole, but join_use_nulls widens only the keys that can be
-- inside Nullable. A key that cannot must keep the type it has, not be rejected as an illegal argument.
DROP TABLE IF EXISTS t_infer_map_l;
DROP TABLE IF EXISTS t_infer_map_r;

CREATE TABLE t_infer_map_l (mlk UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY mlk;
CREATE TABLE t_infer_map_r (mrk UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY mrk;

INSERT INTO t_infer_map_l SELECT number % 100, map('x', number % 7) FROM numbers(10000);
INSERT INTO t_infer_map_r SELECT number, map('x', number % 7) FROM numbers(100);

-- The Map key is read back, so it stays in the join output where its type is looked up.
SELECT '-- composite key whose second member cannot be inside Nullable: mlk is still inferred';
SELECT substring(explain, position(explain, 'Condition:')) FROM (
    EXPLAIN PLAN indexes = 1
    SELECT count(), uniqExact(r.m) FROM t_infer_map_l AS l
    LEFT JOIN t_infer_map_r AS r ON l.mlk = r.mrk AND l.m = r.m
    WHERE r.mrk = 42
    SETTINGS join_use_nulls = 1
) WHERE explain LIKE '%Condition:%';

SELECT count(), uniqExact(r.m) FROM t_infer_map_l AS l
LEFT JOIN t_infer_map_r AS r ON l.mlk = r.mrk AND l.m = r.m WHERE r.mrk = 42
SETTINGS join_use_nulls = 1;
SELECT count(), uniqExact(r.m) FROM t_infer_map_l AS l
LEFT JOIN t_infer_map_r AS r ON l.mlk = r.mrk AND l.m = r.m WHERE r.mrk = 42
SETTINGS join_use_nulls = 0;

DROP TABLE t_infer_map_l;
DROP TABLE t_infer_map_r;

-- A Tuple key is widened, and its cast is built, but Nullable(Tuple) is not usable as a key condition.
-- The inferred predicate must still be correct where it is evaluated.
DROP TABLE IF EXISTS t_infer_tup_l;
DROP TABLE IF EXISTS t_infer_tup_r;

CREATE TABLE t_infer_tup_l (tlk Tuple(UInt64, String)) ENGINE = MergeTree ORDER BY tlk;
CREATE TABLE t_infer_tup_r (trk Tuple(UInt64, String)) ENGINE = MergeTree ORDER BY trk;

INSERT INTO t_infer_tup_l SELECT (number % 100, toString(number % 100)) FROM numbers(10000);
INSERT INTO t_infer_tup_r SELECT (number, toString(number)) FROM numbers(100);

-- Casting the left key to the widened type is what proves the inferred predicate reached the left input,
-- since the condition below cannot show it.
SELECT '-- Tuple key: the left key is cast to the widened type, so the inferred predicate is pushed';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_infer_tup_l AS l
    LEFT JOIN t_infer_tup_r AS r ON l.tlk = r.trk
    WHERE r.trk = (42, '42')
    SETTINGS join_use_nulls = 1
) WHERE explain LIKE '%CAST(tlk %Nullable(Tuple(UInt64, String))%';

SELECT '-- and it prunes nothing: the left condition stays true';
SELECT substring(explain, position(explain, 'Condition:')) FROM (
    EXPLAIN PLAN indexes = 1
    SELECT count() FROM t_infer_tup_l AS l
    LEFT JOIN t_infer_tup_r AS r ON l.tlk = r.trk
    WHERE r.trk = (42, '42')
    SETTINGS join_use_nulls = 1
) WHERE explain LIKE '%Condition:%';

SELECT '-- Tuple key, join_use_nulls 1 then 0';
SELECT count() FROM t_infer_tup_l AS l
LEFT JOIN t_infer_tup_r AS r ON l.tlk = r.trk WHERE r.trk = (42, '42')
SETTINGS join_use_nulls = 1;
SELECT count() FROM t_infer_tup_l AS l
LEFT JOIN t_infer_tup_r AS r ON l.tlk = r.trk WHERE r.trk = (42, '42')
SETTINGS join_use_nulls = 0;

DROP TABLE t_infer_tup_l;
DROP TABLE t_infer_tup_r;

-- A LowCardinality key is widened to LowCardinality(Nullable(...)), which is usable as a key condition.
DROP TABLE IF EXISTS t_infer_lc_l;
DROP TABLE IF EXISTS t_infer_lc_r;

CREATE TABLE t_infer_lc_l (clk LowCardinality(String)) ENGINE = MergeTree ORDER BY clk;
CREATE TABLE t_infer_lc_r (crk LowCardinality(String)) ENGINE = MergeTree ORDER BY crk;

INSERT INTO t_infer_lc_l SELECT toString(number % 100) FROM numbers(10000);
INSERT INTO t_infer_lc_r SELECT toString(number) FROM numbers(100);

SELECT '-- LowCardinality key, join_use_nulls = 1: clk is inferred';
SELECT substring(explain, position(explain, 'Condition:')) FROM (
    EXPLAIN PLAN indexes = 1
    SELECT count() FROM t_infer_lc_l AS l
    LEFT JOIN t_infer_lc_r AS r ON l.clk = r.crk
    WHERE r.crk = '42'
    SETTINGS join_use_nulls = 1
) WHERE explain LIKE '%Condition:%';

SELECT count() FROM t_infer_lc_l AS l
LEFT JOIN t_infer_lc_r AS r ON l.clk = r.crk WHERE r.crk = '42'
SETTINGS join_use_nulls = 1;
SELECT count() FROM t_infer_lc_l AS l
LEFT JOIN t_infer_lc_r AS r ON l.clk = r.crk WHERE r.crk = '42'
SETTINGS join_use_nulls = 0;

DROP TABLE t_infer_lc_l;
DROP TABLE t_infer_lc_r;
