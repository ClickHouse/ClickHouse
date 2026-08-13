SET enable_analyzer = 1;

DROP TABLE IF EXISTS pw_lam_u32;
DROP TABLE IF EXISTS pw_lam_nul;
DROP TABLE IF EXISTS pw_lam_same_a;
DROP TABLE IF EXISTS pw_lam_same_b;
DROP TABLE IF EXISTS pw_lam_one;
DROP TABLE IF EXISTS pw_lam_join_l;
DROP TABLE IF EXISTS pw_lam_join_r;
DROP TABLE IF EXISTS pw_lam_aj;
DROP TABLE IF EXISTS pw_lam_lj_a;
DROP TABLE IF EXISTS pw_lam_lj_b;

-- Two Merge children declaring x with different types: the Merge contract excludes x
-- from PREWHERE, so referencing it must be refused however it is spelled.
CREATE TABLE pw_lam_u32 (x UInt32)           ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE pw_lam_nul (x Nullable(UInt32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO pw_lam_u32 VALUES (100);
INSERT INTO pw_lam_nul VALUES (300);

SELECT 'unsupported column inside a lambda body, qualified';
SELECT x FROM merge(currentDatabase(), '^pw_lam_u32$|^pw_lam_nul$') AS m
PREWHERE (arrayMap(x -> m.x, [1])[1]) > 50 ORDER BY x; -- { serverError ILLEGAL_PREWHERE }

SELECT 'unsupported column inside a lambda body, asterisk';
SELECT * FROM merge(currentDatabase(), '^pw_lam_u32$|^pw_lam_nul$') AS m
PREWHERE (arrayMap(x -> *, [1])[1]) > 50 ORDER BY x; -- { serverError ILLEGAL_PREWHERE }

SELECT 'unsupported column outside a lambda, refused already';
SELECT x FROM merge(currentDatabase(), '^pw_lam_u32$|^pw_lam_nul$') AS m
PREWHERE m.x > 50 ORDER BY x; -- { serverError ILLEGAL_PREWHERE }

SELECT 'higher-order function other than arrayMap';
SELECT x FROM merge(currentDatabase(), '^pw_lam_u32$|^pw_lam_nul$') AS m
PREWHERE arrayFilter(x -> m.x > 50, [1]) != []; -- { serverError ILLEGAL_PREWHERE }

-- PREWHERE runs at the storage read, before ARRAY JOIN, so an array-joined value is not
-- readable there. Both spellings must be refused alike.
CREATE TABLE pw_lam_aj (x UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO pw_lam_aj VALUES (100);

SELECT 'array-joined alias inside a lambda body';
SELECT x FROM pw_lam_aj ARRAY JOIN [1, 2] AS arr
PREWHERE (arrayMap(y -> arr, [1])[1]) > 0 ORDER BY x; -- { serverError ILLEGAL_PREWHERE }

SELECT 'array-joined alias outside a lambda, refused already';
SELECT x FROM pw_lam_aj ARRAY JOIN [1, 2] AS arr
PREWHERE arr > 0 ORDER BY x; -- { serverError ILLEGAL_PREWHERE }

-- Children agreeing on the type keep x in the contract, so the lambda still reads it.
CREATE TABLE pw_lam_same_a (x UInt32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE pw_lam_same_b (x UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO pw_lam_same_a VALUES (100);
INSERT INTO pw_lam_same_b VALUES (300);

SELECT 'supported column inside a lambda body';
SELECT x FROM merge(currentDatabase(), '^pw_lam_same_a$|^pw_lam_same_b$') AS m
PREWHERE (arrayMap(x -> m.x, [1])[1]) > 50 ORDER BY x;

SELECT 'diverging types are fine in WHERE';
SELECT x FROM merge(currentDatabase(), '^pw_lam_u32$|^pw_lam_nul$') AS m
WHERE (arrayMap(x -> m.x, [1])[1]) > 50 ORDER BY x;

-- A storage with no restriction admits every column.
CREATE TABLE pw_lam_one (x UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO pw_lam_one VALUES (100);

SELECT 'unrestricted storage, lambda body';
SELECT x FROM pw_lam_one PREWHERE (arrayMap(x -> pw_lam_one.x, [1])[1]) > 50 ORDER BY x;

SELECT 'lambda parameter shadowing a table column';
SELECT x FROM pw_lam_one PREWHERE (arrayMap(x -> x + 1, [1])[1]) > 0 ORDER BY x;

-- A lambda referencing a column of the second table expression must resolve to that
-- table instead of being pinned to the first one. PREWHERE filters before the JOIN.
CREATE TABLE pw_lam_join_l (id UInt32, av UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE pw_lam_join_r (id UInt32, bv UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO pw_lam_join_l VALUES (1, 10), (2, 20);
INSERT INTO pw_lam_join_r VALUES (1, 100), (2, 200);

SELECT 'lambda over a non-first table expression';
SELECT l.id, l.av, r.bv FROM pw_lam_join_l AS l JOIN pw_lam_join_r AS r ON l.id = r.id
PREWHERE (arrayMap(y -> r.bv, [1])[1]) > 150 ORDER BY l.id;

-- Issue #114206: a LEFT JOIN keeps left rows with no match, so the lambda spelling must agree
-- with the non-lambda PREWHERE spelling below, not with the post-join WHERE spelling.
CREATE TABLE pw_lam_lj_a (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE pw_lam_lj_b (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO pw_lam_lj_a VALUES (1, 10), (2, 20);
INSERT INTO pw_lam_lj_b VALUES (1, 100);

SELECT 'left join, right column inside a lambda body';
SELECT a.id FROM pw_lam_lj_a AS a LEFT JOIN pw_lam_lj_b AS b ON a.id = b.x
PREWHERE arrayExists(z -> b.y != 0, [1]) ORDER BY a.id;

SELECT 'left join, right column outside a lambda';
SELECT a.id FROM pw_lam_lj_a AS a LEFT JOIN pw_lam_lj_b AS b ON a.id = b.x
PREWHERE b.y != 0 ORDER BY a.id;

SELECT 'left join, right column in WHERE, filtered after the join';
SELECT a.id FROM pw_lam_lj_a AS a LEFT JOIN pw_lam_lj_b AS b ON a.id = b.x
WHERE arrayExists(z -> b.y != 0, [1]) ORDER BY a.id;

-- join_use_nulls makes the right column Nullable, and PREWHERE must see its original type.
-- Both spellings therefore agree with each other and with the join_use_nulls = 0 rows above.
SELECT 'left join with join_use_nulls, right column inside a lambda body';
SELECT a.id FROM pw_lam_lj_a AS a LEFT JOIN pw_lam_lj_b AS b ON a.id = b.x
PREWHERE arrayExists(z -> b.y != 0, [1]) ORDER BY a.id SETTINGS join_use_nulls = 1;

SELECT 'left join with join_use_nulls, right column outside a lambda';
SELECT a.id FROM pw_lam_lj_a AS a LEFT JOIN pw_lam_lj_b AS b ON a.id = b.x
PREWHERE b.y != 0 ORDER BY a.id SETTINGS join_use_nulls = 1;

-- The parameter types of a nested lambda are derived from the array it iterates, so restoring a
-- column inside that array must re-derive them. Every spelling and depth agrees with the
-- non-lambda control above.
SELECT 'left join with join_use_nulls, nested lambda over the right column';
SELECT a.id FROM pw_lam_lj_a AS a LEFT JOIN pw_lam_lj_b AS b ON a.id = b.x
PREWHERE arrayExists(z -> arrayExists(y -> y != 0, [b.y]), [1]) ORDER BY a.id SETTINGS join_use_nulls = 1;

SELECT 'nested lambda over the right column, without join_use_nulls';
SELECT a.id FROM pw_lam_lj_a AS a LEFT JOIN pw_lam_lj_b AS b ON a.id = b.x
PREWHERE arrayExists(z -> arrayExists(y -> y != 0, [b.y]), [1]) ORDER BY a.id SETTINGS join_use_nulls = 0;

SELECT 'nested arrayMap over the right column';
SELECT a.id FROM pw_lam_lj_a AS a LEFT JOIN pw_lam_lj_b AS b ON a.id = b.x
PREWHERE arrayExists(z -> (arrayMap(y -> y != 0, [b.y])[1]), [1]) ORDER BY a.id SETTINGS join_use_nulls = 1;

SELECT 'nested arrayFilter over the right column';
SELECT a.id FROM pw_lam_lj_a AS a LEFT JOIN pw_lam_lj_b AS b ON a.id = b.x
PREWHERE arrayExists(z -> arrayFilter(y -> y != 0, [b.y]) != [], [1]) ORDER BY a.id SETTINGS join_use_nulls = 1;

SELECT 'three lambdas deep over the right column';
SELECT a.id FROM pw_lam_lj_a AS a LEFT JOIN pw_lam_lj_b AS b ON a.id = b.x
PREWHERE arrayExists(w -> arrayExists(z -> arrayExists(y -> y != 0, [b.y]), [1]), [1])
ORDER BY a.id SETTINGS join_use_nulls = 1;

-- Nothing is restored without a join, so the same nesting reads the column unchanged.
SELECT 'nested lambda over a column of a single table';
SELECT v FROM pw_lam_lj_a PREWHERE arrayExists(z -> arrayExists(y -> y != 0, [pw_lam_lj_a.v]), [1]) ORDER BY v;

-- PREWHERE reads a single table expression, so a lambda body spanning both join inputs is
-- refused like the non-lambda spelling instead of reaching the reader.
SELECT 'lambda body reading both join inputs';
SELECT a.id FROM pw_lam_lj_a AS a LEFT JOIN pw_lam_lj_b AS b ON a.id = b.x
PREWHERE arrayExists(z -> (a.id = 1) AND (b.y != 0), [1]) ORDER BY a.id; -- { serverError ILLEGAL_PREWHERE }

SELECT 'both join inputs outside a lambda, refused already';
SELECT a.id FROM pw_lam_lj_a AS a LEFT JOIN pw_lam_lj_b AS b ON a.id = b.x
PREWHERE (a.id = 1) AND (b.y != 0) ORDER BY a.id; -- { serverError ILLEGAL_PREWHERE }

-- A lambda parameter is bound by the lambda, so sharing a name and a type with the
-- substituted join column must not make the restoration rewrite it.
SELECT 'left join with join_use_nulls, lambda parameter shadowing the right column';
SELECT a.id, b.y FROM pw_lam_lj_a AS a LEFT JOIN pw_lam_lj_b AS b ON a.id = b.x
PREWHERE arrayExists(y -> (y = 7) AND (b.y != 0), [toNullable(toUInt64(7))])
ORDER BY a.id SETTINGS join_use_nulls = 1;

DROP TABLE pw_lam_u32;
DROP TABLE pw_lam_nul;
DROP TABLE pw_lam_same_a;
DROP TABLE pw_lam_same_b;
DROP TABLE pw_lam_one;
DROP TABLE pw_lam_aj;
DROP TABLE pw_lam_join_l;
DROP TABLE pw_lam_join_r;
DROP TABLE pw_lam_lj_a;
DROP TABLE pw_lam_lj_b;
