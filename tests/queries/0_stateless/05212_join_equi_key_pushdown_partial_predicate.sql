-- A conjunct over an equi-join key is classified pushable to BOTH join inputs and copied to the
-- opposite side with that key column substituted, so the copy runs on values the side the conjunct
-- names never held. Only a conjunct that is total on its argument types may be copied that way.

SET enable_analyzer = 1;
SET query_plan_filter_push_down = 1;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET allow_experimental_dynamic_type = 1;
SET allow_dynamic_type_in_join_keys = 1;

DROP TABLE IF EXISTS pd_like_src;
DROP TABLE IF EXISTS pd_like_dst;
DROP TABLE IF EXISTS pd_num_src;
DROP TABLE IF EXISTS pd_num_dst;
DROP TABLE IF EXISTS pd_dec_src;
DROP TABLE IF EXISTS pd_dec_dst;
DROP TABLE IF EXISTS pd_dyn_src;
DROP TABLE IF EXISTS pd_dyn_dst;
DROP TABLE IF EXISTS pk_tup_src;
DROP TABLE IF EXISTS pk_tup_dst;

-- A single backslash is not a valid LIKE pattern, and only the destination holds it.
CREATE TABLE pd_like_src (c0 String) ENGINE = Memory;
CREATE TABLE pd_like_dst (c0 String) ENGINE = Memory;
INSERT INTO pd_like_src VALUES ('a');
INSERT INTO pd_like_dst VALUES ('a'), ('\\');

-- Only the destination holds a value `toUInt64` cannot parse.
CREATE TABLE pd_num_src (c0 String) ENGINE = Memory;
CREATE TABLE pd_num_dst (c0 String) ENGINE = Memory;
INSERT INTO pd_num_src VALUES ('1');
INSERT INTO pd_num_dst VALUES ('1'), ('abc');

-- Only the destination holds a value that overflows a rescale to scale 10.
CREATE TABLE pd_dec_src (k Decimal(38, 0)) ENGINE = Memory;
CREATE TABLE pd_dec_dst (k Decimal(38, 0)) ENGINE = Memory;
INSERT INTO pd_dec_src VALUES (1);
INSERT INTO pd_dec_dst VALUES (1), (99999999999999999999999999999999);

CREATE TABLE pd_dyn_src (k Dynamic) ENGINE = Memory;
CREATE TABLE pd_dyn_dst (k Dynamic) ENGINE = Memory;
INSERT INTO pd_dyn_src VALUES (0);
INSERT INTO pd_dyn_dst VALUES (0), ('x');

-- `propagatePredicateAcrossEquiJoin` only copies a conjunct onto a MergeTree primary key.
CREATE TABLE pk_tup_src (c0 String) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE pk_tup_dst (c0 String) ENGINE = MergeTree ORDER BY c0;
INSERT INTO pk_tup_src VALUES ('(1)');
INSERT INTO pk_tup_dst VALUES ('(1)'), ('abc');

-- 1. The reported query: `LIKE` parses its pattern per row.
SELECT c0
FROM (SELECT pd_like_src.c0 AS c0, (pd_like_src.c0 LIKE pd_like_src.c0) AS ref
      FROM pd_like_src INNER JOIN pd_like_dst ON (pd_like_src.c0 = pd_like_dst.c0)) AS s
WHERE ref;

-- 2. The rejected conjunct still filters the side that names it: one leg, not zero and not two.
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

-- 3. Partiality is the property, not `LIKE`: `equals` is admitted, the `toUInt64` under it is not.
SELECT c0
FROM (SELECT pd_num_src.c0 AS c0, (toUInt64(pd_num_src.c0) = 1) AS ref
      FROM pd_num_src INNER JOIN pd_num_dst ON (pd_num_src.c0 = pd_num_dst.c0)) AS s
WHERE ref;

-- 4. Comparing decimals of different scales rescales one side per row, which can overflow.
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

-- 5. A total conjunct is still copied to both legs: the optimization is narrowed, not removed.
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

-- 6. `not` is total by name, but a `Dynamic` argument resolves the real function per stored alternative
-- at execution time, so an alternative only the destination holds throws there.
SELECT count()
FROM (SELECT pd_dyn_src.k AS k, (NOT pd_dyn_src.k) AS ref
      FROM pd_dyn_src INNER JOIN pd_dyn_dst ON (pd_dyn_src.k = pd_dyn_dst.k)) AS s
WHERE ref;

-- 7. The same rule in `propagatePredicateAcrossEquiJoin`: a set lookup whose key type is not the probe
-- column's own casts the probe per row, so it must not be substituted onto the other side either.
SELECT s.c0
FROM (SELECT pk_tup_src.c0 AS c0 FROM pk_tup_src INNER JOIN pk_tup_dst ON pk_tup_src.c0 = pk_tup_dst.c0) AS s
WHERE s.c0 IN (SELECT tuple(toUInt64(1)))
SETTINGS query_plan_propagate_predicate_across_join = 1;

-- 8. That pass still substitutes a set lookup whose key type is the probe column's own.
SELECT count()
FROM
(
    EXPLAIN actions = 1
    SELECT count()
    FROM (SELECT * FROM pk_tup_src WHERE c0 IN ('(1)')) AS s
    INNER JOIN pk_tup_dst AS d ON s.c0 = d.c0
    SETTINGS query_plan_propagate_predicate_across_join = 1
)
WHERE explain ILIKE '%Propagated equi-join filter%';

DROP TABLE pd_like_src;
DROP TABLE pd_like_dst;
DROP TABLE pd_num_src;
DROP TABLE pd_num_dst;
DROP TABLE pd_dec_src;
DROP TABLE pd_dec_dst;
DROP TABLE pd_dyn_src;
DROP TABLE pd_dyn_dst;
DROP TABLE pk_tup_src;
DROP TABLE pk_tup_dst;
