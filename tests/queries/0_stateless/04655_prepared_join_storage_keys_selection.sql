-- A `Join` table engine can only be joined on the key list fixed at its creation, so a `WHERE`
-- equality on any other column must stay a filter instead of being promoted to a second join key.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_merge_filter_into_join_condition = 1; -- CI may inject False

CREATE TABLE l (id UInt64, tag Int32) ENGINE = Memory;
CREATE TABLE sj_same (id UInt64, tag Int32) ENGINE = Join(ALL, INNER, id);
CREATE TABLE sj_null (id UInt64, tag Nullable(Int32)) ENGINE = Join(ALL, INNER, id);
CREATE TABLE sj_left (id UInt64, tag Int32) ENGINE = Join(ALL, LEFT, id);

INSERT INTO l VALUES (1, 5), (2, 7);
INSERT INTO sj_same VALUES (1, 5), (2, 9);
INSERT INTO sj_null VALUES (1, 5), (2, 9);
INSERT INTO sj_left VALUES (1, 5), (2, 9);

SELECT '-- non-key equality in WHERE, operand types equal';
SELECT * FROM l ALL INNER JOIN sj_same ON l.id = sj_same.id WHERE l.tag = sj_same.tag ORDER BY ALL;
SELECT * FROM l, sj_same WHERE l.id = sj_same.id AND l.tag = sj_same.tag ORDER BY ALL;

SELECT '-- non-key equality in WHERE, operand types differ but have a common supertype';
SELECT * FROM l ALL INNER JOIN sj_null ON l.id = sj_null.id WHERE l.tag = sj_null.tag ORDER BY ALL;
SELECT * FROM l, sj_null WHERE l.id = sj_null.id AND l.tag = sj_null.tag ORDER BY ALL;

SELECT '-- non-key equality in WHERE, with USING and with join_use_nulls';
SELECT * FROM l ALL INNER JOIN sj_same USING (id) WHERE l.tag = sj_same.tag ORDER BY ALL;
SELECT * FROM l ALL INNER JOIN sj_same ON l.id = sj_same.id WHERE l.tag = sj_same.tag ORDER BY ALL SETTINGS join_use_nulls = 1;

SELECT '-- an OUTER join converted to INNER by the WHERE condition';
SELECT * FROM l ALL LEFT JOIN sj_left ON l.id = sj_left.id WHERE l.tag = sj_left.tag ORDER BY ALL;

SELECT '-- joining on the engine key alone is unaffected';
SELECT * FROM l ALL INNER JOIN sj_same ON l.id = sj_same.id ORDER BY ALL;

SELECT '-- joining without the engine key is still rejected';
SELECT * FROM l ALL INNER JOIN sj_same ON l.tag = sj_same.tag; -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
SELECT * FROM l, sj_same WHERE l.tag = sj_same.tag; -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }

SELECT '-- a non-key equality in ON of an OUTER join is rejected';
SELECT * FROM l ALL LEFT JOIN sj_left ON l.id = sj_left.id AND l.tag = sj_left.tag; -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
