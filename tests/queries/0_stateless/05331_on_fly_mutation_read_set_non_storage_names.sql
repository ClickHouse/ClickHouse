-- Tags: no-shared-catalog
-- no-shared-catalog: STOP MERGES will only stop them on the current replica, the second one will
-- continue to merge and can materialize the mutation this test needs to stay pending

-- A pending command may name things that are not storage columns: an ALIAS column, a lambda
-- parameter, an alias defined in the expression, a table-qualified name, a flattened Nested group.
-- On-fly reads must resolve them to the storage columns they read, and close over the earlier
-- commands that write those columns. Each arm is read once on the fly and once materialized.

SET alter_sync = 0, mutations_sync = 0;
SET apply_mutations_on_fly = 1;
SET flatten_nested = 1;

SELECT 'alias in a delete predicate';

DROP TABLE IF EXISTS t_alias_delete;

CREATE TABLE t_alias_delete (id UInt8, al UInt64 ALIAS id * 2) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_alias_delete (id) VALUES (1), (2);
SYSTEM STOP MERGES t_alias_delete;

ALTER TABLE t_alias_delete DELETE WHERE al = 2;
SELECT count() FROM t_alias_delete;
SELECT groupArray(id) FROM t_alias_delete;

DELETE FROM t_alias_delete WHERE al = 4 SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 0;
SELECT count() FROM t_alias_delete;

SYSTEM START MERGES t_alias_delete;
ALTER TABLE t_alias_delete UPDATE id = id WHERE 1 SETTINGS mutations_sync = 2;
SELECT count() FROM t_alias_delete;

DROP TABLE t_alias_delete;

SELECT 'alias in an update expression, closed over its source';

DROP TABLE IF EXISTS t_alias_update;

CREATE TABLE t_alias_update
(
    id UInt8, b UInt64, e UInt64, f UInt64, c UInt64,
    al UInt64 ALIAS c + 1, al2 UInt64 ALIAS al + 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_alias_update (id, b, e, c) VALUES (1, 0, 0, 5), (2, 0, 0, 6);
SYSTEM STOP MERGES t_alias_update;

ALTER TABLE t_alias_update UPDATE c = 10 WHERE 1;
-- Inside the definition of `al`, `c` is the column, not the parameter of the enclosing lambda.
ALTER TABLE t_alias_update UPDATE b = al, e = arraySum(arrayMap(c -> c + al, [0])), f = al2 WHERE 1;
SELECT id, b FROM t_alias_update ORDER BY id;
SELECT id, e FROM t_alias_update ORDER BY id;
SELECT id, f FROM t_alias_update ORDER BY id;

SYSTEM START MERGES t_alias_update;
ALTER TABLE t_alias_update UPDATE c = c WHERE 1 SETTINGS mutations_sync = 2;
SELECT id, b FROM t_alias_update ORDER BY id;
SELECT id, e FROM t_alias_update ORDER BY id;
SELECT id, f FROM t_alias_update ORDER BY id;

DROP TABLE t_alias_update;

SELECT 'alias of an alias, alias over a subcolumn';

DROP TABLE IF EXISTS t_alias_chain;

CREATE TABLE t_alias_chain
(
    id UInt8,
    a Array(UInt32),
    al UInt64 ALIAS a.size0,
    al2 UInt64 ALIAS id * 2,
    al3 UInt64 ALIAS al2 + 1,
    b UInt64,
    c UInt64
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_alias_chain (id, a) VALUES (1, [1, 2]), (2, [3]);
SYSTEM STOP MERGES t_alias_chain;

ALTER TABLE t_alias_chain UPDATE b = al, c = al3 WHERE 1;
SELECT id, b, c FROM t_alias_chain ORDER BY id;

SYSTEM START MERGES t_alias_chain;
ALTER TABLE t_alias_chain UPDATE b = b WHERE 1 SETTINGS mutations_sync = 2;
SELECT id, b, c FROM t_alias_chain ORDER BY id;

DROP TABLE t_alias_chain;

SELECT 'lambda parameters and expression aliases';

DROP TABLE IF EXISTS t_lambda;

CREATE TABLE t_lambda (id UInt8, a Array(UInt32), b UInt64, c UInt64, d UInt64, x UInt64)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lambda VALUES (1, [1, 2], 0, 0, 0, 1), (2, [3], 0, 0, 0, 2);
SYSTEM STOP MERGES t_lambda;

ALTER TABLE t_lambda UPDATE x = 100 WHERE 1;
-- Outside the lambda body `x` is the column again, so the update of `x` has to be applied.
ALTER TABLE t_lambda UPDATE
    b = arraySum(arrayMap(v -> v * 2, a)),
    c = (id + 1 AS z) * z,
    d = arraySum(arrayMap(x -> x * 2, a)) + x
WHERE 1;
SELECT id, b FROM t_lambda ORDER BY id;
SELECT id, c FROM t_lambda ORDER BY id;
SELECT id, d FROM t_lambda ORDER BY id;

SYSTEM START MERGES t_lambda;
ALTER TABLE t_lambda UPDATE x = x WHERE 1 SETTINGS mutations_sync = 2;
SELECT id, b FROM t_lambda ORDER BY id;
SELECT id, c FROM t_lambda ORDER BY id;
SELECT id, d FROM t_lambda ORDER BY id;

DROP TABLE t_lambda;

SELECT 'qualified name, element of a tuple alias, both';

DROP TABLE IF EXISTS t_qualified;

CREATE TABLE t_qualified
(
    id UInt8,
    b UInt64,
    d UInt64,
    e UInt64,
    c UInt64,
    al Tuple(x UInt64, y UInt64) ALIAS (c, c + 1)
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_qualified (id, b, d, e, c) VALUES (1, 0, 0, 0, 5), (2, 0, 0, 0, 6);
SYSTEM STOP MERGES t_qualified;

ALTER TABLE t_qualified UPDATE c = 10 WHERE 1;
-- Only the assignments to the selected columns survive, so each SELECT checks one spelling.
ALTER TABLE t_qualified UPDATE b = t_qualified.c, d = t_qualified.al.x, e = al.y WHERE 1;
SELECT id, b FROM t_qualified ORDER BY id;
SELECT id, d FROM t_qualified ORDER BY id;
SELECT id, e FROM t_qualified ORDER BY id;

SYSTEM START MERGES t_qualified;
ALTER TABLE t_qualified UPDATE c = c WHERE 1 SETTINGS mutations_sync = 2;
SELECT id, b FROM t_qualified ORDER BY id;
SELECT id, d FROM t_qualified ORDER BY id;
SELECT id, e FROM t_qualified ORDER BY id;

DROP TABLE t_qualified;

SELECT 'flattened nested group';

DROP TABLE IF EXISTS t_nested_group;

CREATE TABLE t_nested_group (id UInt8, b UInt64, n Nested(x UInt8, y UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_nested_group (id, b, n.x, n.y) VALUES (1, 0, [1, 2], [3, 4]), (2, 0, [5], [6]);
SYSTEM STOP MERGES t_nested_group;

ALTER TABLE t_nested_group UPDATE `n.x` = [7, 8, 9], `n.y` = [1, 2, 3] WHERE id = 1;
ALTER TABLE t_nested_group UPDATE b = length(n) WHERE 1;
SELECT id, b FROM t_nested_group ORDER BY id;

SYSTEM START MERGES t_nested_group;
ALTER TABLE t_nested_group UPDATE b = b WHERE 1 SETTINGS mutations_sync = 2;
SELECT id, b FROM t_nested_group ORDER BY id;

DROP TABLE t_nested_group;

SELECT 'alias over a subcolumn of an updated column';

DROP TABLE IF EXISTS t_alias_subcolumn;
DROP TABLE IF EXISTS t_alias_subcolumn_snapshot;

CREATE TABLE t_alias_subcolumn (id UInt8, a Array(UInt32), al UInt64 ALIAS a.size0, b UInt64)
ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_alias_subcolumn_snapshot (id UInt8, b UInt64) ENGINE = Memory;
INSERT INTO t_alias_subcolumn (id, a, b) VALUES (1, [1, 2], 0), (2, [3], 0);
SYSTEM STOP MERGES t_alias_subcolumn;

ALTER TABLE t_alias_subcolumn UPDATE a = [7, 8, 9] WHERE id = 1;
ALTER TABLE t_alias_subcolumn UPDATE b = al WHERE 1;
INSERT INTO t_alias_subcolumn_snapshot SELECT id, b FROM t_alias_subcolumn;

SYSTEM START MERGES t_alias_subcolumn;
ALTER TABLE t_alias_subcolumn UPDATE b = b WHERE 1 SETTINGS mutations_sync = 2;
-- The on-fly read returns what the mutation stores.
SELECT (SELECT groupArray((id, b)) FROM (SELECT * FROM t_alias_subcolumn_snapshot ORDER BY id))
    = (SELECT groupArray((id, b)) FROM (SELECT id, b FROM t_alias_subcolumn ORDER BY id));

DROP TABLE t_alias_subcolumn;
DROP TABLE t_alias_subcolumn_snapshot;

SELECT 'names that only look like a column do not read it';

DROP TABLE IF EXISTS t_shadow;

CREATE TABLE t_shadow
(
    id UInt8, a Array(UInt32), tup Tuple(x UInt64, y UInt64),
    x UInt64, y UInt64, t_shadow UInt64, b UInt64, d UInt64, e UInt64,
    `_part` Nested(a UInt64), p UInt8
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_shadow (id, a, tup) VALUES (1, [1, 2], (10, 20)), (2, [3], (30, 40));
SYSTEM STOP MERGES t_shadow;

-- Fails when applied, so a read that pulls it in fails too.
ALTER TABLE t_shadow UPDATE
    x = x + throwIf(1), y = y + throwIf(1), t_shadow = t_shadow + throwIf(1), `_part.a` = [throwIf(1)]
WHERE 1;
ALTER TABLE t_shadow UPDATE b = arraySum(arrayMap(x -> x * 2, a)), d = t_shadow.id, e = tup.y, p = notEmpty(_part) WHERE 1;
SELECT id, x FROM t_shadow ORDER BY id; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT id, b FROM t_shadow ORDER BY id;
SELECT id, d FROM t_shadow ORDER BY id;
SELECT id, e FROM t_shadow ORDER BY id;
SELECT id, p FROM t_shadow ORDER BY id;

KILL MUTATION WHERE database = currentDatabase() AND table = 't_shadow' AND command LIKE '%throwIf%' SYNC FORMAT Null;
SYSTEM START MERGES t_shadow;
ALTER TABLE t_shadow UPDATE b = b WHERE 1 SETTINGS mutations_sync = 2;
SELECT id, b FROM t_shadow ORDER BY id;
SELECT id, d FROM t_shadow ORDER BY id;
SELECT id, e FROM t_shadow ORDER BY id;
SELECT id, p FROM t_shadow ORDER BY id;

DROP TABLE t_shadow;

SELECT 'alias over a nondeterministic function is not applied on the fly';

DROP TABLE IF EXISTS t_alias_rand;

CREATE TABLE t_alias_rand (id UInt8, b UInt64, c UInt64, al UInt64 ALIAS rand(), al2 UInt64 ALIAS al + 1)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_alias_rand (id, b, c) VALUES (1, 0, 0), (2, 0, 0);
SYSTEM STOP MERGES t_alias_rand;

ALTER TABLE t_alias_rand UPDATE b = al, c = al2 WHERE 1;
SELECT id, b FROM t_alias_rand ORDER BY id; -- { serverError BAD_ARGUMENTS }
SELECT id, c FROM t_alias_rand ORDER BY id; -- { serverError BAD_ARGUMENTS }
SELECT id FROM t_alias_rand ORDER BY id;
SELECT id, b, c FROM t_alias_rand ORDER BY id SETTINGS apply_mutations_on_fly = 0;

KILL MUTATION WHERE database = currentDatabase() AND table = 't_alias_rand' SYNC FORMAT Null;
DROP TABLE t_alias_rand;

SELECT 'a qualified name reads only the column the analyzer binds it to';

DROP TABLE IF EXISTS t_collision;

CREATE TABLE t_collision (
    id UInt8, k UInt64, t_collision Tuple(k UInt64), b UInt64, al Tuple(k UInt64) ALIAS tuple(k), `al.k` UInt64, d UInt64)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_collision (id, k, t_collision, `al.k`) VALUES (1, 1, tuple(7), 0), (2, 2, tuple(8), 0);
SYSTEM STOP MERGES t_collision;

ALTER TABLE t_collision UPDATE k = k + throwIf(1) WHERE 1;
ALTER TABLE t_collision UPDATE b = t_collision.k, d = al.k WHERE 1;
-- `t_collision.k` is the tuple element by default and the column `k` with the compatibility setting.
SELECT id, b FROM t_collision ORDER BY id SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 0;
SELECT id, b FROM t_collision ORDER BY id SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
-- `al.k` is the column `al.k`, not the element of the ALIAS `al`.
SELECT id, d FROM t_collision ORDER BY id;

KILL MUTATION WHERE database = currentDatabase() AND table = 't_collision' AND command LIKE '%throwIf%' SYNC FORMAT Null;
SYSTEM START MERGES t_collision;
ALTER TABLE t_collision UPDATE b = b WHERE 1 SETTINGS mutations_sync = 2;
SELECT id, b FROM t_collision ORDER BY id;
SELECT id, d FROM t_collision ORDER BY id;

DROP TABLE t_collision;

SELECT 'an ALIAS prefix and a Nested group bind what the analyzer binds';

DROP TABLE IF EXISTS t_prefix;

CREATE TABLE t_prefix
(
    id UInt8, zzz UInt64, t_prefix UInt64 ALIAS id * 2, `n.a` Array(UInt8), `n.scalar` UInt64,
    b UInt64, `n.c` Array(UInt8), `n.x.y` Array(UInt8), c UInt64, `t_prefix.n.x` Array(UInt8), d UInt64
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_prefix (id, zzz, `n.a`, `n.c`, `n.x.y`, `t_prefix.n.x`) VALUES (1, 1, [1], [1], [1], [0]), (2, 2, [2], [2], [2], [0]);
SYSTEM STOP MERGES t_prefix;

ALTER TABLE t_prefix UPDATE zzz = 10, `n.a` = [1, 2, 3], `n.c` = [1, 2, 3], `n.x.y` = [1, 2, 3] WHERE id = 1;
-- Fails when applied, so a read that pulls it in fails too.
ALTER TABLE t_prefix UPDATE `n.scalar` = `n.scalar` + throwIf(1), `t_prefix.n.x` = [throwIf(1)] WHERE 1;
ALTER TABLE t_prefix UPDATE b = t_prefix.zzz, c = length(n), d = length(t_prefix.n) WHERE 1;
-- The ALIAS `t_prefix` has no element `zzz`, so `t_prefix.zzz` is the column `zzz`.
SELECT id, b FROM t_prefix ORDER BY id;
-- `n` is the group of the Array columns `n.a`, `n.c` and `n.x.y`, and so is `t_prefix.n`.
SELECT id, c FROM t_prefix ORDER BY id;
SELECT id, d FROM t_prefix ORDER BY id;

KILL MUTATION WHERE database = currentDatabase() AND table = 't_prefix' AND command LIKE '%throwIf%' SYNC FORMAT Null;
SYSTEM START MERGES t_prefix;
ALTER TABLE t_prefix UPDATE b = b WHERE 1 SETTINGS mutations_sync = 2;
SELECT id, b FROM t_prefix ORDER BY id;
SELECT id, c FROM t_prefix ORDER BY id;
SELECT id, d FROM t_prefix ORDER BY id;

DROP TABLE t_prefix;
