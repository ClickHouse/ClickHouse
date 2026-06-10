-- Tags: distributed

-- Test for fix: multiple ALIAS columns with the same underlying expression
-- must produce independent expression trees when queried via Distributed.
-- Previously, setAlias on the shared expression node would overwrite the alias
-- set by a prior column, causing NUMBER_OF_COLUMNS_DOESNT_MATCH or wrong results.
-- The bug requires aliases that resolve to the SAME expression node (not just
-- the same source column with different transformations).

DROP TABLE IF EXISTS test_local;

CREATE TABLE test_local
(
    `id` UInt64,
    `value` String,
    `alias1` String ALIAS value,
    `alias2` String ALIAS value
)
ENGINE = Memory;

INSERT INTO test_local VALUES (1, 'Hello'), (2, 'World'), (3, 'Test');

-- S1: Core regression — two aliases with identical expression + ORDER BY
SELECT alias1, alias2
FROM remote('127.0.0.{1,2}', currentDatabase(), test_local)
ORDER BY alias1;

-- S2: Three aliases sharing the same expression (multiple overwrites)
DROP TABLE IF EXISTS test_local_three;

CREATE TABLE test_local_three
(
    `id` UInt64,
    `x` UInt64,
    `a1` UInt64 ALIAS x,
    `a2` UInt64 ALIAS x,
    `a3` UInt64 ALIAS x
)
ENGINE = Memory;

INSERT INTO test_local_three VALUES (1, 10), (2, 20), (3, 30);

SELECT a1, a2, a3
FROM remote('127.0.0.{1,2}', currentDatabase(), test_local_three)
ORDER BY a1;

-- S3: Physical column mixed with identical-expression aliases + ORDER BY
SELECT id, alias1, alias2
FROM remote('127.0.0.{1,2}', currentDatabase(), test_local)
ORDER BY id;

-- S4: Aliases with identical function expressions
DROP TABLE IF EXISTS test_local_func;

CREATE TABLE test_local_func
(
    `id` UInt64,
    `value` String,
    `u1` String ALIAS upper(value),
    `u2` String ALIAS upper(value)
)
ENGINE = Memory;

INSERT INTO test_local_func VALUES (1, 'Hello'), (2, 'World'), (3, 'Test');

SELECT u1, u2
FROM remote('127.0.0.{1,2}', currentDatabase(), test_local_func)
ORDER BY u1;

DROP TABLE IF EXISTS test_local;
DROP TABLE IF EXISTS test_local_three;
DROP TABLE IF EXISTS test_local_func;
