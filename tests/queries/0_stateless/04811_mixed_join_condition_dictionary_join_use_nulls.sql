-- A mixed JOIN ON condition - a cross-side non-equi residual such as
-- ON (t.key = d.key) AND (t.a * 10 < d.a) - is evaluated by the hash family only, during matching,
-- over the right columns as they were stored. `DirectKeyValueJoin` declines it, so a join onto a
-- dictionary that carries one falls through to a hash algorithm.
--
-- With `join_use_nulls = 1` the right side of such a join used to be pre-converted to `Nullable` by
-- the right-side expression, under the same column names the mixed condition is built on. `HashJoin`
-- resolves those by name, so it read a `Nullable` column through the non-`Nullable` interface the
-- condition declared: an aborted assertion in a debug build, and arbitrary matching in a release one.

SET enable_analyzer = 1;
SET allow_experimental_join_condition = 1;

DROP TABLE IF EXISTS dsrc;
DROP DICTIONARY IF EXISTS dict;
DROP TABLE IF EXISTS t;

CREATE TABLE dsrc (key UInt64, a UInt32) ENGINE = Memory;
INSERT INTO dsrc VALUES (1, 100), (2, 20);
CREATE DICTIONARY dict (key UInt64, a UInt32) PRIMARY KEY key SOURCE(CLICKHOUSE(TABLE 'dsrc')) LIFETIME(0) LAYOUT(FLAT());

CREATE TABLE t (key UInt64, a UInt32) ENGINE = Memory;
INSERT INTO t VALUES (1, 1), (2, 2), (3, 3);

-- Of the three left rows only key 1 satisfies the residual (1 * 10 < 100); key 2 shares the key but
-- fails it (2 * 10 < 20 is false) and key 3 is absent from the dictionary. So a LEFT join keeps all
-- three rows with one match, and dropping or misreading the residual would match key 2 as well.
SELECT 'oracle', t.key, t.a, dsrc.a FROM t LEFT ANY JOIN dsrc ON (t.key = dsrc.key) AND (t.a * 10 < dsrc.a)
ORDER BY t.key SETTINGS join_use_nulls = 1, join_algorithm = 'direct,parallel_hash,hash';

SELECT '-- dictionary, join_use_nulls = 1 --';

SELECT 'default list', t.key, t.a, d.a FROM t LEFT ANY JOIN dict AS d ON (t.key = d.key) AND (t.a * 10 < d.a)
ORDER BY t.key SETTINGS join_use_nulls = 1, join_algorithm = 'direct,parallel_hash,hash';

SELECT 'hash', t.key, t.a, d.a FROM t LEFT ANY JOIN dict AS d ON (t.key = d.key) AND (t.a * 10 < d.a)
ORDER BY t.key SETTINGS join_use_nulls = 1, join_algorithm = 'hash';

SELECT 'parallel_hash', t.key, t.a, d.a FROM t LEFT ANY JOIN dict AS d ON (t.key = d.key) AND (t.a * 10 < d.a)
ORDER BY t.key SETTINGS join_use_nulls = 1, join_algorithm = 'parallel_hash';

SELECT 'grace_hash', t.key, t.a, d.a FROM t LEFT ANY JOIN dict AS d ON (t.key = d.key) AND (t.a * 10 < d.a)
ORDER BY t.key SETTINGS join_use_nulls = 1, join_algorithm = 'grace_hash';

SELECT 'LEFT ALL', t.key, t.a, d.a FROM t LEFT JOIN dict AS d ON (t.key = d.key) AND (t.a * 10 < d.a)
ORDER BY t.key SETTINGS join_use_nulls = 1, join_algorithm = 'direct,parallel_hash,hash';

SELECT 'LEFT SEMI', t.key, t.a FROM t LEFT SEMI JOIN dict AS d ON (t.key = d.key) AND (t.a * 10 < d.a)
ORDER BY t.key SETTINGS join_use_nulls = 1, join_algorithm = 'direct,parallel_hash,hash';

SELECT 'LEFT ANTI', t.key, t.a FROM t LEFT ANTI JOIN dict AS d ON (t.key = d.key) AND (t.a * 10 < d.a)
ORDER BY t.key SETTINGS join_use_nulls = 1, join_algorithm = 'direct,parallel_hash,hash';

-- INNER ALL turns the residual into a post-join filter instead of a mixed condition, so it keeps the
-- matched row only and never reaches the mixed-condition path.
SELECT 'INNER ALL', t.key, t.a, d.a FROM t INNER JOIN dict AS d ON (t.key = d.key) AND (t.a * 10 < d.a)
ORDER BY t.key SETTINGS join_use_nulls = 1, join_algorithm = 'direct,parallel_hash,hash';

SELECT '-- dictionary, join_use_nulls = 0: unmatched rows get the attribute default, not NULL --';

SELECT 'default list', t.key, t.a, d.a FROM t LEFT ANY JOIN dict AS d ON (t.key = d.key) AND (t.a * 10 < d.a)
ORDER BY t.key SETTINGS join_use_nulls = 0, join_algorithm = 'direct,parallel_hash,hash';

SELECT 'hash', t.key, t.a, d.a FROM t LEFT ANY JOIN dict AS d ON (t.key = d.key) AND (t.a * 10 < d.a)
ORDER BY t.key SETTINGS join_use_nulls = 0, join_algorithm = 'hash';

SELECT '-- a plain equi join onto the dictionary still uses the direct join --';

SELECT 'direct', countIf(explain LIKE '%FilledJoin%') FROM
(
    EXPLAIN PIPELINE SELECT count() FROM t LEFT ANY JOIN dict AS d ON t.key = d.key
    SETTINGS join_use_nulls = 1, join_algorithm = 'direct,parallel_hash,hash', query_plan_optimize_join_order_randomize = 0
);

SELECT 'direct value', t.key, t.a, d.a FROM t LEFT ANY JOIN dict AS d ON t.key = d.key
ORDER BY t.key SETTINGS join_use_nulls = 1, join_algorithm = 'direct,parallel_hash,hash';

DROP DICTIONARY dict;
DROP TABLE dsrc;
DROP TABLE t;
