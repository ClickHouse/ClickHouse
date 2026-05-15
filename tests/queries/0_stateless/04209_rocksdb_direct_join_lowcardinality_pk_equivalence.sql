-- Tags: use-rocksdb

DROP TABLE IF EXISTS rdb_lc_pk;
DROP TABLE IF EXISTS rdb_lc_null_pk;
DROP TABLE IF EXISTS t_str_right;

CREATE TABLE rdb_lc_pk (key LowCardinality(String), value String)
ENGINE = EmbeddedRocksDB PRIMARY KEY (key);
INSERT INTO rdb_lc_pk VALUES ('a', 'A'), ('b', 'B'), ('c', 'C');

CREATE TABLE rdb_lc_null_pk (key LowCardinality(Nullable(String)), value String)
ENGINE = EmbeddedRocksDB PRIMARY KEY (key);
INSERT INTO rdb_lc_null_pk VALUES ('a', 'A'), ('b', 'B'), ('c', 'C');

CREATE TABLE t_str_right (k String) ENGINE = TinyLog;
INSERT INTO t_str_right VALUES ('a'), ('b'), ('c'), ('d');

-- INNER JOIN: LowCardinality(String) PK vs plain String right key must still
-- pick DirectKeyValueJoin.
SELECT 'inner join result';
SELECT key, value FROM (SELECT k AS key FROM t_str_right) AS t
INNER JOIN rdb_lc_pk USING (key)
ORDER BY key;

SELECT 'inner join explain';
SELECT trim(explain) FROM (
    EXPLAIN actions = 1
    SELECT key, value FROM (SELECT k AS key FROM t_str_right) AS t
    INNER JOIN rdb_lc_pk USING (key)
)
WHERE explain LIKE '%Algorithm:%';

-- LEFT JOIN: same equivalence path, different `allowed_left` strictness branch.
SELECT 'left join result';
SELECT key, value FROM (SELECT k AS key FROM t_str_right) AS t
LEFT JOIN rdb_lc_pk USING (key)
ORDER BY key;

SELECT 'left join explain';
SELECT trim(explain) FROM (
    EXPLAIN actions = 1
    SELECT key, value FROM (SELECT k AS key FROM t_str_right) AS t
    LEFT JOIN rdb_lc_pk USING (key)
)
WHERE explain LIKE '%Algorithm:%';

-- INNER JOIN ON: covers the plain ON path (no USING-cast). Result check
-- guards against a future regression where the right-side cast renames the
-- key and forces the planner to fall back to HashJoin.
SELECT 'inner join on result';
SELECT t.key, rdb_lc_pk.value FROM (SELECT k AS key FROM t_str_right) AS t
INNER JOIN rdb_lc_pk ON t.key = rdb_lc_pk.key
ORDER BY t.key;

SELECT 'inner join on explain';
SELECT trim(explain) FROM (
    EXPLAIN actions = 1
    SELECT t.key, rdb_lc_pk.value FROM (SELECT k AS key FROM t_str_right) AS t
    INNER JOIN rdb_lc_pk ON t.key = rdb_lc_pk.key
)
WHERE explain LIKE '%Algorithm:%';

-- LowCardinality(Nullable(String)) PK: exercises the recursive
-- `removeNullable(recursiveRemoveLowCardinality(...))` stripping on both sides.
-- Plain String right key vs LC(Nullable(String)) PK -> stripped types both
-- `String`, so the right-side cast is skipped and DirectKeyValueJoin is picked.
SELECT 'lc(nullable) inner join result';
SELECT key, value FROM (SELECT k AS key FROM t_str_right) AS t
INNER JOIN rdb_lc_null_pk USING (key)
ORDER BY key;

SELECT 'lc(nullable) inner join explain';
SELECT trim(explain) FROM (
    EXPLAIN actions = 1
    SELECT key, value FROM (SELECT k AS key FROM t_str_right) AS t
    INNER JOIN rdb_lc_null_pk USING (key)
)
WHERE explain LIKE '%Algorithm:%';

SELECT 'lc(nullable) inner join on result';
SELECT t.key, rdb_lc_null_pk.value FROM (SELECT k AS key FROM t_str_right) AS t
INNER JOIN rdb_lc_null_pk ON t.key = rdb_lc_null_pk.key
ORDER BY t.key;

SELECT 'lc(nullable) inner join on explain';
SELECT trim(explain) FROM (
    EXPLAIN actions = 1
    SELECT t.key, rdb_lc_null_pk.value FROM (SELECT k AS key FROM t_str_right) AS t
    INNER JOIN rdb_lc_null_pk ON t.key = rdb_lc_null_pk.key
)
WHERE explain LIKE '%Algorithm:%';

DROP TABLE rdb_lc_pk;
DROP TABLE rdb_lc_null_pk;
DROP TABLE t_str_right;
