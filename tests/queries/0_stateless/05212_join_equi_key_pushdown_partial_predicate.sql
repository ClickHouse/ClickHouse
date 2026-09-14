-- A conjunct over an equi-join key is classified pushable to BOTH join inputs and copied to the
-- opposite side with that key column substituted, so the copy runs on values the side the conjunct
-- names never held. Only a conjunct that is total on its argument types may be copied that way.

SET enable_analyzer = 1;
SET query_plan_filter_push_down = 1;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;

DROP TABLE IF EXISTS pd_like_src;
DROP TABLE IF EXISTS pd_like_dst;
DROP TABLE IF EXISTS pd_num_src;
DROP TABLE IF EXISTS pd_num_dst;
DROP TABLE IF EXISTS pd_dyn_src;
DROP TABLE IF EXISTS pd_dyn_dst;
DROP TABLE IF EXISTS pd_dec_src;
DROP TABLE IF EXISTS pd_dec_dst;
DROP TABLE IF EXISTS pd_tup_src;
DROP TABLE IF EXISTS pd_tup_dst;

-- The destination row is not a join partner of anything in the source, and its value is what the
-- predicate cannot process: a single backslash is not a valid LIKE pattern.
CREATE TABLE pd_like_src (c0 String) ENGINE = Memory;
CREATE TABLE pd_like_dst (c0 String) ENGINE = Memory;
INSERT INTO pd_like_src VALUES ('a');
INSERT INTO pd_like_dst VALUES ('a'), ('\\');

CREATE TABLE pd_num_src (c0 String) ENGINE = Memory;
CREATE TABLE pd_num_dst (c0 String) ENGINE = Memory;
INSERT INTO pd_num_src VALUES ('1');
INSERT INTO pd_num_dst VALUES ('1'), ('abc');

CREATE TABLE pd_dec_src (k Decimal(38, 0)) ENGINE = Memory;
CREATE TABLE pd_dec_dst (k Decimal(38, 0)) ENGINE = Memory;
INSERT INTO pd_dec_src VALUES (1);
INSERT INTO pd_dec_dst VALUES (1), (99999999999999999999999999999999);

CREATE TABLE pd_tup_src (c0 String) ENGINE = Memory;
CREATE TABLE pd_tup_dst (c0 String) ENGINE = Memory;
INSERT INTO pd_tup_src VALUES ('(1)');
INSERT INTO pd_tup_dst VALUES ('(1)'), ('abc');

-- 1. The reported query: the predicate is projected in a derived table and filtered above the join.
SELECT c0
FROM (SELECT pd_like_src.c0 AS c0, (pd_like_src.c0 LIKE pd_like_src.c0) AS ref
      FROM pd_like_src INNER JOIN pd_like_dst ON (pd_like_src.c0 = pd_like_dst.c0)) AS s
WHERE ref;

-- 2. The same predicate written as a plain WHERE next to the join. It substitutes onto the other
-- table of the pair, so it is not case 1 again.
SELECT pd_like_src.c0
FROM pd_like_src INNER JOIN pd_like_dst ON (pd_like_src.c0 = pd_like_dst.c0)
WHERE (pd_like_src.c0 LIKE pd_like_src.c0);

-- 2b. The property is partiality, not LIKE: `equals` is admitted, so this conjunct is refused only
-- because the whole conjunct is walked and `toUInt64` is not admitted.
SELECT c0
FROM (SELECT pd_num_src.c0 AS c0, (toUInt64(pd_num_src.c0) = 1) AS ref
      FROM pd_num_src INNER JOIN pd_num_dst ON (pd_num_src.c0 = pd_num_dst.c0)) AS s
WHERE ref;

-- 3. The substituted copy is gone from the plan and the source-side copy is not: one leg, not two.
SELECT count()
FROM
(
    EXPLAIN actions = 1
    SELECT c0
    FROM (SELECT pd_like_src.c0 AS c0, (pd_like_src.c0 LIKE pd_like_src.c0) AS ref
          FROM pd_like_src INNER JOIN pd_like_dst ON (pd_like_src.c0 = pd_like_dst.c0)) AS s
    WHERE ref
)
WHERE explain ILIKE '%Filter column: c0 LIKE c0%';

-- 4. A total predicate is still copied to both legs: the optimization is narrowed, not removed.
SELECT count()
FROM
(
    EXPLAIN actions = 1
    SELECT c0
    FROM (SELECT pd_like_src.c0 AS c0, (pd_like_src.c0 = 'a') AS ref
          FROM pd_like_src INNER JOIN pd_like_dst ON (pd_like_src.c0 = pd_like_dst.c0)) AS s
    WHERE ref
)
WHERE explain ILIKE '%Filter column: c0 = %';

-- 5. A `Dynamic` argument makes the real function be built per stored alternative at execution time,
-- so an alternative only the destination holds throws there. `not` is admitted by name, so the type
-- of the argument is the only thing that can refuse this.
SET allow_experimental_dynamic_type = 1;
SET allow_dynamic_type_in_join_keys = 1;
CREATE TABLE pd_dyn_src (k Dynamic) ENGINE = Memory;
CREATE TABLE pd_dyn_dst (k Dynamic) ENGINE = Memory;
INSERT INTO pd_dyn_src VALUES (0);
INSERT INTO pd_dyn_dst VALUES (0), ('x');

SELECT count()
FROM (SELECT pd_dyn_src.k AS k, (NOT pd_dyn_src.k) AS ref
      FROM pd_dyn_src INNER JOIN pd_dyn_dst ON (pd_dyn_src.k = pd_dyn_dst.k)) AS s
WHERE ref;

-- 6. A set lookup casts the probe column into the set's key type per row, so a set whose key type
-- differs from the probe column's is refused: one leg, not two.
SELECT count()
FROM
(
    EXPLAIN actions = 1
    SELECT c0
    FROM (SELECT pd_num_src.c0 AS c0, (pd_num_src.c0 IN (SELECT toUInt64(1))) AS ref
          FROM pd_num_src INNER JOIN pd_num_dst ON (pd_num_src.c0 = pd_num_dst.c0)) AS s
    WHERE ref
)
WHERE explain ILIKE '%Filter column: c0 IN subquery1%';

-- 6b. `transform_null_in` renames the function to `nullIn`, which is not admitted at all; that is
-- the shape in which a set lookup's per-row cast is reached and throws.
SELECT c0
FROM (SELECT pd_num_src.c0 AS c0, (pd_num_src.c0 IN (SELECT toUInt64(1))) AS ref
      FROM pd_num_src INNER JOIN pd_num_dst ON (pd_num_src.c0 = pd_num_dst.c0)) AS s
WHERE ref
SETTINGS transform_null_in = 1;

-- 7. A comparison of two decimals of different scales rescales one side per row, which can overflow,
-- so the comparison is refused: one leg, not two.
SELECT count()
FROM
(
    EXPLAIN actions = 1
    SELECT k
    FROM (SELECT pd_dec_src.k AS k, (pd_dec_src.k = toDecimal128('1.0', 10)) AS ref
          FROM pd_dec_src INNER JOIN pd_dec_dst ON (pd_dec_src.k = pd_dec_dst.k)) AS s
    WHERE ref
)
WHERE explain ILIKE '%Filter column: k = %';

-- 8. A set whose key type is already the probe column's own is still copied to both legs, because
-- the per-row cast is then an identity. The destination-only row is kept: it is the row that would
-- be parsed if the types were not equal.
SELECT c0
FROM (SELECT pd_num_src.c0 AS c0, (pd_num_src.c0 IN (SELECT '1')) AS ref
      FROM pd_num_src INNER JOIN pd_num_dst ON (pd_num_src.c0 = pd_num_dst.c0)) AS s
WHERE ref;

SELECT c0
FROM (SELECT pd_num_src.c0 AS c0, (pd_num_src.c0 IN (SELECT '1')) AS ref
      FROM pd_num_src INNER JOIN pd_num_dst ON (pd_num_src.c0 = pd_num_dst.c0)) AS s
WHERE ref
SETTINGS query_plan_filter_push_down = 0;

SELECT count()
FROM
(
    EXPLAIN actions = 1
    SELECT c0
    FROM (SELECT pd_num_src.c0 AS c0, (pd_num_src.c0 IN (SELECT '1')) AS ref
          FROM pd_num_src INNER JOIN pd_num_dst ON (pd_num_src.c0 = pd_num_dst.c0)) AS s
    WHERE ref
)
WHERE explain ILIKE '%Filter column: c0 IN subquery1%';

-- 9. A `Tuple` set key type reaches the throwing arm of the set lookup with `transform_null_in` at
-- its default, so an `IN` is not total by name alone.
SELECT c0
FROM (SELECT pd_tup_src.c0 AS c0, (pd_tup_src.c0 IN (SELECT tuple(toUInt64(1)))) AS ref
      FROM pd_tup_src INNER JOIN pd_tup_dst ON (pd_tup_src.c0 = pd_tup_dst.c0)) AS s
WHERE ref;

DROP TABLE pd_like_src;
DROP TABLE pd_like_dst;
DROP TABLE pd_num_src;
DROP TABLE pd_num_dst;
DROP TABLE pd_dyn_src;
DROP TABLE pd_dyn_dst;
DROP TABLE pd_dec_src;
DROP TABLE pd_dec_dst;
DROP TABLE pd_tup_src;
DROP TABLE pd_tup_dst;
