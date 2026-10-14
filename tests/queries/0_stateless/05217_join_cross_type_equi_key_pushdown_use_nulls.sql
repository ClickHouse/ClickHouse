-- A predicate over one side of a cross-type equi key is substituted by the opposite side's key and pushed
-- below the JOIN. The substituted key keeps its own name when it already has the keys' supertype, so that
-- name has to denote a single type at that point in the plan: `join_use_nulls` republishes the JOIN's own
-- input names at the widened output type, and the pushed predicate binds its inputs by name.

SET enable_analyzer = 1;
SET join_use_nulls = 1;

DROP TABLE IF EXISTS t_pushdown_u8;
DROP TABLE IF EXISTS t_pushdown_u16;
DROP TABLE IF EXISTS t_pushdown_u64;

CREATE TABLE t_pushdown_u8 (id UInt8, value String) ENGINE = Memory;
CREATE TABLE t_pushdown_u16 (id UInt16, value String) ENGINE = Memory;
CREATE TABLE t_pushdown_u64 (id UInt64, value String) ENGINE = Memory;

INSERT INTO t_pushdown_u8 VALUES (0, 'u8_0'), (1, 'u8_1'), (2, 'u8_2');
INSERT INTO t_pushdown_u16 VALUES (0, 'u16_0'), (1, 'u16_1'), (3, 'u16_3');
INSERT INTO t_pushdown_u64 VALUES (0, 'u64_0'), (1, 'u64_1'), (4, 'u64_4');

SELECT 'widened right input';

-- The middle key is republished as Nullable because a LEFT JOIN null-extends it, and it is also the
-- substitute for the left key, whose predicate is pushed to that side.
SELECT id, u16.id, u16.value
FROM t_pushdown_u8 AS u8 LEFT JOIN t_pushdown_u16 AS u16 USING (id)
                         RIGHT JOIN t_pushdown_u64 AS u64 USING (id)
WHERE id > 1
ORDER BY ALL;

SELECT 'widened left input';

-- The mirror direction: a RIGHT JOIN null-extends the left key, which substitutes the right key here.
SELECT u64.id, u16.id, u64.value
FROM t_pushdown_u64 AS u64 RIGHT JOIN t_pushdown_u16 AS u16 USING (id)
WHERE u16.id > 1
ORDER BY ALL;

SELECT 'in-range control: types the JOIN does not change';

-- Without the null extension the key name denotes one type, and the substitution keeps that bare name.
SELECT id, u16.id, u16.value
FROM t_pushdown_u8 AS u8 LEFT JOIN t_pushdown_u16 AS u16 USING (id)
                         RIGHT JOIN t_pushdown_u64 AS u64 USING (id)
WHERE id > 1
ORDER BY ALL
SETTINGS join_use_nulls = 0;

SELECT 'push-down is live';

-- A cross-type substitution reaches both joined tables as a cast predicate, and nothing is pushed when
-- the optimization is off. The runtime filter and the outer-to-inner rewrite are pinned because they add
-- their own filters to the plan.
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT id, u16.id, u16.value
    FROM t_pushdown_u8 AS u8 LEFT JOIN t_pushdown_u16 AS u16 USING (id)
                             RIGHT JOIN t_pushdown_u64 AS u64 USING (id)
    WHERE id > 1
    SETTINGS query_plan_filter_push_down = 1, enable_join_runtime_filters = 0,
             query_plan_convert_outer_join_to_inner_join = 1
) WHERE explain ILIKE '%Filter column: CAST(%';

SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT id, u16.id, u16.value
    FROM t_pushdown_u8 AS u8 LEFT JOIN t_pushdown_u16 AS u16 USING (id)
                             RIGHT JOIN t_pushdown_u64 AS u64 USING (id)
    WHERE id > 1
    SETTINGS query_plan_filter_push_down = 0, enable_join_runtime_filters = 0,
             query_plan_convert_outer_join_to_inner_join = 1
) WHERE explain ILIKE '%Filter column: CAST(%';

SELECT 'renamed replacement converts nothing';

-- A renamed substitute whose type already matches is an alias, so the predicate reaching the middle table
-- casts the key once. The outer key, widened by both USING clauses in turn, is the one cast twice.
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT id, u16.id, u16.value
    FROM t_pushdown_u8 AS u8 LEFT JOIN t_pushdown_u16 AS u16 USING (id)
                             RIGHT JOIN t_pushdown_u64 AS u64 USING (id)
    WHERE id > 1
    SETTINGS query_plan_filter_push_down = 1, enable_join_runtime_filters = 0,
             query_plan_convert_outer_join_to_inner_join = 1
) WHERE explain ILIKE '%Filter column: CAST(id AS UInt64) > 1%';

DROP TABLE IF EXISTS t_pushdown_lc;
DROP TABLE IF EXISTS t_pushdown_str;
DROP TABLE IF EXISTS t_pushdown_str_2;

CREATE TABLE t_pushdown_lc (id LowCardinality(String), value String) ENGINE = Memory;
CREATE TABLE t_pushdown_str (id String, value String) ENGINE = Memory;
CREATE TABLE t_pushdown_str_2 (id String, value String) ENGINE = Memory;

INSERT INTO t_pushdown_lc VALUES ('a', 'lc_a'), ('b', 'lc_b');
INSERT INTO t_pushdown_str VALUES ('a', 'str_a'), ('c', 'str_c');
INSERT INTO t_pushdown_str_2 VALUES ('a', 'str2_a'), ('d', 'str2_d');

SELECT 'low cardinality key pair';

-- The supertype of LowCardinality(String) and String is String, so the plain String side is the one that
-- carries the supertype and substitutes the other, and the null extension widens it.
SELECT id, str.id, str.value
FROM t_pushdown_lc AS lc LEFT JOIN t_pushdown_str AS str USING (id)
                         RIGHT JOIN t_pushdown_str_2 AS str_2 USING (id)
WHERE id > 'a'
ORDER BY ALL;

DROP TABLE t_pushdown_u8;
DROP TABLE t_pushdown_u16;
DROP TABLE t_pushdown_u64;
DROP TABLE t_pushdown_lc;
DROP TABLE t_pushdown_str;
DROP TABLE t_pushdown_str_2;
