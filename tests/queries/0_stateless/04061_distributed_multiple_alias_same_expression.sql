-- Tags: distributed

-- Test for fix: multiple ALIAS columns sharing the same physical column
-- should produce independent expression trees when queried via Distributed.
-- Previously, setAlias on a shared expression node would overwrite aliases,
-- causing NUMBER_OF_COLUMNS_DOESNT_MATCH or wrong results.

DROP TABLE IF EXISTS test_local;

CREATE TABLE test_local
(
    `id` UInt64,
    `value` String,
    `alias1` String ALIAS upper(value),
    `alias2` String ALIAS lower(value)
)
ENGINE = Memory;

INSERT INTO test_local VALUES (1, 'Hello'), (2, 'World'), (3, 'Test');

-- S1: Multiple aliases sharing the same physical column + ORDER BY (core regression)
SELECT alias1, alias2
FROM remote('127.0.0.{1,2}', currentDatabase(), test_local)
ORDER BY alias1;

-- S2: Multiple aliases without ORDER BY (regression, use FORMAT Null to avoid row order instability)
SELECT alias1, alias2
FROM remote('127.0.0.{1,2}', currentDatabase(), test_local)
FORMAT Null;

-- S3: Function expression aliases + ORDER BY
DROP TABLE IF EXISTS test_local_func;

CREATE TABLE test_local_func
(
    `x` UInt64,
    `double_x` UInt64 ALIAS x * 2,
    `triple_x` UInt64 ALIAS x * 3
)
ENGINE = Memory;

INSERT INTO test_local_func VALUES (1), (2), (3);

SELECT double_x, triple_x
FROM remote('127.0.0.{1,2}', currentDatabase(), test_local_func)
ORDER BY double_x;

-- S4: Physical column + alias mix + ORDER BY
SELECT id, alias1, alias2
FROM remote('127.0.0.{1,2}', currentDatabase(), test_local)
ORDER BY id;

-- S5: Multiple aliases + ORDER BY + LIMIT
SELECT alias1, alias2
FROM remote('127.0.0.{1,2}', currentDatabase(), test_local)
ORDER BY alias1
LIMIT 2;

-- S6: Different-type aliases sharing the same source column
DROP TABLE IF EXISTS test_local_types;

CREATE TABLE test_local_types
(
    `src` String,
    `alias_str` String ALIAS src,
    `alias_len` UInt64 ALIAS length(src)
)
ENGINE = Memory;

INSERT INTO test_local_types VALUES ('abc'), ('de'), ('f');

SELECT alias_str, alias_len
FROM remote('127.0.0.{1,2}', currentDatabase(), test_local_types)
ORDER BY alias_str;

DROP TABLE IF EXISTS test_local;
DROP TABLE IF EXISTS test_local_func;
DROP TABLE IF EXISTS test_local_types;
