-- Tags: distributed

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/81631
-- Incorrect SELECT results due to mixed order of columns from Distributed engine.
-- The `Change remote column names to local column names` step uses MatchColumnsMode::Position
-- which misaligns columns when the remote returns them in a different order than expected locally.

-- Basic ALIAS columns with shared subexpressions (the original bug report).
DROP TABLE IF EXISTS local_t;
DROP TABLE IF EXISTS dist_t;

CREATE TABLE local_t
(
    `dt` DateTime,
    `flags_bitmap` UInt8,
    `flag_zero` Bool ALIAS toBool(bitTest(flags_bitmap, 0)),
    `flag_one` Bool ALIAS toBool(bitTest(flags_bitmap, 1))
)
ENGINE = MergeTree()
ORDER BY dt;

CREATE TABLE dist_t
(
    `dt` DateTime,
    `flags_bitmap` UInt8,
    `flag_zero` Bool ALIAS toBool(bitTest(flags_bitmap, 0)),
    `flag_one` Bool ALIAS toBool(bitTest(flags_bitmap, 1))
)
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_t, rand());

INSERT INTO local_t VALUES ('2024-01-01 00:00:00', 1); -- bit0=1, bit1=0

SELECT 'local';
SELECT flag_zero, flag_one, bitTest(flags_bitmap, 0) AS x FROM local_t ORDER BY dt DESC LIMIT 1;

SELECT 'distributed';
SELECT flag_zero, flag_one, bitTest(flags_bitmap, 0) AS x FROM dist_t ORDER BY dt DESC LIMIT 1;

DROP TABLE dist_t;
DROP TABLE local_t;

-- Shared subexpressions: flag_a's expansion contains flag_b's expansion.
DROP TABLE IF EXISTS local_sub;
DROP TABLE IF EXISTS dist_sub;

CREATE TABLE local_sub
(
    `dt` DateTime,
    `x` UInt8,
    `flag_a` String ALIAS concat(toString(x), '_suffix'),
    `flag_b` String ALIAS toString(x)
)
ENGINE = MergeTree()
ORDER BY dt;

CREATE TABLE dist_sub
(
    `dt` DateTime,
    `x` UInt8,
    `flag_a` String ALIAS concat(toString(x), '_suffix'),
    `flag_b` String ALIAS toString(x)
)
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_sub, rand());

INSERT INTO local_sub VALUES ('2024-01-01 00:00:00', 42);

SELECT 'local_sub';
SELECT flag_a, flag_b FROM local_sub ORDER BY dt DESC LIMIT 1;

SELECT 'distributed_sub';
SELECT flag_a, flag_b FROM dist_sub ORDER BY dt DESC LIMIT 1;

DROP TABLE dist_sub;
DROP TABLE local_sub;

-- Nested aliases: a2 depends on a1 which depends on x.
DROP TABLE IF EXISTS local_nested;
DROP TABLE IF EXISTS dist_nested;

CREATE TABLE local_nested
(
    `dt` DateTime,
    `x` UInt8,
    `a1` UInt16 ALIAS x + 1,
    `a2` UInt16 ALIAS a1 + 1
)
ENGINE = MergeTree()
ORDER BY dt;

CREATE TABLE dist_nested
(
    `dt` DateTime,
    `x` UInt8,
    `a1` UInt16 ALIAS x + 1,
    `a2` UInt16 ALIAS a1 + 1
)
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_nested, rand());

INSERT INTO local_nested VALUES ('2024-01-01 00:00:00', 10);

SELECT 'local_nested';
SELECT a1, a2 FROM local_nested ORDER BY dt DESC LIMIT 1;

SELECT 'distributed_nested';
SELECT a1, a2 FROM dist_nested ORDER BY dt DESC LIMIT 1;

DROP TABLE dist_nested;
DROP TABLE local_nested;

-- ORDER BY expression over alias column.
DROP TABLE IF EXISTS local_expr;
DROP TABLE IF EXISTS dist_expr;

CREATE TABLE local_expr
(
    `dt` DateTime,
    `x` UInt8,
    `flag_a` String ALIAS concat(toString(x), '_suffix'),
    `flag_b` String ALIAS toString(x)
)
ENGINE = MergeTree()
ORDER BY dt;

CREATE TABLE dist_expr
(
    `dt` DateTime,
    `x` UInt8,
    `flag_a` String ALIAS concat(toString(x), '_suffix'),
    `flag_b` String ALIAS toString(x)
)
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_expr, rand());

INSERT INTO local_expr VALUES ('2024-01-01 00:00:00', 42);

SELECT 'local_expr';
SELECT flag_a, flag_b FROM local_expr ORDER BY flag_a || '_extra' LIMIT 1;

SELECT 'distributed_expr';
SELECT flag_a, flag_b FROM dist_expr ORDER BY flag_a || '_extra' LIMIT 1;

DROP TABLE dist_expr;
DROP TABLE local_expr;

-- Alias names that are prefixes of each other (flag, flag1).
DROP TABLE IF EXISTS local_prefix;
DROP TABLE IF EXISTS dist_prefix;

CREATE TABLE local_prefix
(
    `dt` DateTime,
    `x` UInt8,
    `flag` String ALIAS concat(toString(x), '_suffix'),
    `flag1` String ALIAS toString(x)
)
ENGINE = MergeTree()
ORDER BY dt;

CREATE TABLE dist_prefix
(
    `dt` DateTime,
    `x` UInt8,
    `flag` String ALIAS concat(toString(x), '_suffix'),
    `flag1` String ALIAS toString(x)
)
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_prefix, rand());

INSERT INTO local_prefix VALUES ('2024-01-01 00:00:00', 42);

SELECT 'local_prefix';
SELECT flag, flag1 FROM local_prefix ORDER BY flag || flag1 LIMIT 1;

SELECT 'distributed_prefix';
SELECT flag, flag1 FROM dist_prefix ORDER BY flag || flag1 LIMIT 1;

DROP TABLE dist_prefix;
DROP TABLE local_prefix;

-- Deep nested aliases with ORDER BY expression.
DROP TABLE IF EXISTS local_deep;
DROP TABLE IF EXISTS dist_deep;

CREATE TABLE local_deep
(
    `dt` DateTime,
    `x` UInt64,
    `aa` UInt64 ALIAS x + 1,
    `ac` UInt64 ALIAS aa + 1,
    `ab` UInt64 ALIAS ac + 1
)
ENGINE = MergeTree()
ORDER BY dt;

CREATE TABLE dist_deep
(
    `dt` DateTime,
    `x` UInt64,
    `aa` UInt64 ALIAS x + 1,
    `ac` UInt64 ALIAS aa + 1,
    `ab` UInt64 ALIAS ac + 1
)
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_deep, rand());

INSERT INTO local_deep VALUES ('2024-01-01 00:00:00', 10);

SELECT 'local_deep';
SELECT aa, ab FROM local_deep ORDER BY ab + ac LIMIT 1;

SELECT 'distributed_deep';
SELECT aa, ab FROM dist_deep ORDER BY ab + ac LIMIT 1;

DROP TABLE dist_deep;
DROP TABLE local_deep;

-- Regular column whose name is a prefix of an alias column name (ab vs ab_alias).
-- Ensures the fix does not corrupt non-alias column identifiers.
DROP TABLE IF EXISTS local_prefix_collision;
DROP TABLE IF EXISTS dist_prefix_collision;

CREATE TABLE local_prefix_collision
(
    `dt` DateTime,
    `ab` UInt64,
    `ab_alias` UInt64 ALIAS ab + 100,
    `b` UInt64 ALIAS ab + 200
)
ENGINE = MergeTree()
ORDER BY dt;

CREATE TABLE dist_prefix_collision
(
    `dt` DateTime,
    `ab` UInt64,
    `ab_alias` UInt64 ALIAS ab + 100,
    `b` UInt64 ALIAS ab + 200
)
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_prefix_collision, rand());

INSERT INTO local_prefix_collision VALUES ('2024-01-01 00:00:00', 5);

SELECT 'local_prefix_collision';
SELECT ab, ab_alias, b FROM local_prefix_collision ORDER BY dt DESC LIMIT 1;

SELECT 'distributed_prefix_collision';
SELECT ab, ab_alias, b FROM dist_prefix_collision ORDER BY dt DESC LIMIT 1;

DROP TABLE dist_prefix_collision;
DROP TABLE local_prefix_collision;

-- Duplicate alias in projection mixed with a distinct alias that needs reordering.
-- Ensures the reorder is not disabled when only some names are duplicated.
DROP TABLE IF EXISTS local_dup_mixed;
DROP TABLE IF EXISTS dist_dup_mixed;

CREATE TABLE local_dup_mixed
(
    `dt` DateTime,
    `x` UInt8,
    `flag_a` String ALIAS concat(toString(x), '_suffix'),
    `flag_b` String ALIAS toString(x)
)
ENGINE = MergeTree()
ORDER BY dt;

CREATE TABLE dist_dup_mixed
(
    `dt` DateTime,
    `x` UInt8,
    `flag_a` String ALIAS concat(toString(x), '_suffix'),
    `flag_b` String ALIAS toString(x)
)
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_dup_mixed, rand());

INSERT INTO local_dup_mixed VALUES ('2024-01-01 00:00:00', 42);

SELECT 'local_dup_mixed';
SELECT flag_a, flag_b, flag_b FROM local_dup_mixed ORDER BY dt DESC LIMIT 1;

SELECT 'distributed_dup_mixed';
SELECT flag_a, flag_b, flag_b FROM dist_dup_mixed ORDER BY dt DESC LIMIT 1;

DROP TABLE dist_dup_mixed;
DROP TABLE local_dup_mixed;

-- Two aliases using the same underlying expression (both expand to toString(x)).
-- CSE deduplicates the computation; both output columns should still get correct values.
DROP TABLE IF EXISTS local_same_expr;
DROP TABLE IF EXISTS dist_same_expr;

CREATE TABLE local_same_expr
(
    `dt` DateTime,
    `x` UInt8,
    `a1` String ALIAS toString(x),
    `a2` String ALIAS toString(x)
)
ENGINE = MergeTree()
ORDER BY dt;

CREATE TABLE dist_same_expr
(
    `dt` DateTime,
    `x` UInt8,
    `a1` String ALIAS toString(x),
    `a2` String ALIAS toString(x)
)
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_same_expr, rand());

INSERT INTO local_same_expr VALUES ('2024-01-01 00:00:00', 7);

SELECT 'local_same_expr';
SELECT a1, a2 FROM local_same_expr ORDER BY dt DESC LIMIT 1;

SELECT 'distributed_same_expr';
SELECT a1, a2 FROM dist_same_expr ORDER BY dt DESC LIMIT 1;

DROP TABLE dist_same_expr;
DROP TABLE local_same_expr;
