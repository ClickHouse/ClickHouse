-- Regression test for issue #111340: `transform_null_in = 1`, non-Nullable key column, `IN`/`NOT IN`
-- a subquery whose result is Nullable. Previously threw CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN (349).

SET transform_null_in = 1;

SELECT 'String key IN';
DROP TABLE IF EXISTS t_str;
CREATE TABLE t_str (s String) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_str VALUES ('a'), ('b'), ('c');
SELECT s FROM t_str WHERE s IN (SELECT s FROM t_str UNION ALL SELECT NULL) ORDER BY s;

SELECT 'FixedString key IN';
DROP TABLE IF EXISTS t_fs;
CREATE TABLE t_fs (s FixedString(2)) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_fs VALUES ('ab'), ('cd');
SELECT s FROM t_fs WHERE s IN (SELECT s FROM t_fs UNION ALL SELECT NULL) ORDER BY s;

SELECT 'Int64 key IN';
DROP TABLE IF EXISTS t_int;
CREATE TABLE t_int (s Int64) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_int VALUES (1), (2), (3);
SELECT s FROM t_int WHERE s IN (SELECT s FROM t_int UNION ALL SELECT NULL) ORDER BY s;

SELECT 'LowCardinality(String) key IN';
DROP TABLE IF EXISTS t_lc;
CREATE TABLE t_lc (s LowCardinality(String)) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_lc VALUES ('a'), ('b');
SELECT s FROM t_lc WHERE s IN (SELECT s FROM t_lc UNION ALL SELECT NULL) ORDER BY s;

SELECT 'Date key IN';
DROP TABLE IF EXISTS t_date;
CREATE TABLE t_date (d Date) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_date VALUES ('2020-01-01'), ('2020-01-02');
SELECT d FROM t_date WHERE d IN (SELECT d FROM t_date UNION ALL SELECT NULL) ORDER BY d;

SELECT 'String key IN, transform_null_in=0';
SELECT s FROM t_str WHERE s IN (SELECT s FROM t_str UNION ALL SELECT NULL) ORDER BY s SETTINGS transform_null_in = 0;

SELECT 'Non-PK String column IN';
DROP TABLE IF EXISTS t_nopk;
CREATE TABLE t_nopk (id UInt32, s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_nopk VALUES (1, 'a'), (2, 'b'), (3, 'c');
SELECT s FROM t_nopk WHERE s IN (SELECT s FROM t_nopk UNION ALL SELECT NULL) ORDER BY s;

-- NOT IN must not prune away rows whose value equals the nested default a dropped NULL produces
-- ('' for String, 0 for Int64). The `NULL` set element makes the pruning set inexact, so the
-- partition/index atom must be relaxed; otherwise `NOT IN` prunes the '' / 0 row incorrectly.
SELECT 'String key NOT IN, partition pruning';
DROP TABLE IF EXISTS t_np;
CREATE TABLE t_np (s String) ENGINE = MergeTree ORDER BY s PARTITION BY s;
INSERT INTO t_np VALUES ('a'), ('b'), ('');
SELECT s FROM t_np WHERE s NOT IN (SELECT 'a' UNION ALL SELECT NULL) ORDER BY s;

SELECT 'Int64 key NOT IN, partition pruning';
DROP TABLE IF EXISTS t_ip;
CREATE TABLE t_ip (s Int64) ENGINE = MergeTree ORDER BY s PARTITION BY s;
INSERT INTO t_ip VALUES (5), (7), (0);
SELECT s FROM t_ip WHERE s NOT IN (SELECT 5 UNION ALL SELECT NULL) ORDER BY s;

-- Same relaxation must protect the NOT has() sibling caller (a Nullable array with a NULL element
-- against a non-Nullable partition key). optimize_rewrite_has_to_in=0 keeps it on the has() path.
SELECT 'String key NOT has, partition pruning';
SELECT s FROM t_np WHERE NOT has([CAST('a', 'Nullable(String)'), NULL], s) ORDER BY s SETTINGS optimize_rewrite_has_to_in = 0;

-- A Nullable key must keep working and keep using the set index; NULL on the left matches NULL
-- in the set under transform_null_in=1.
SELECT 'Nullable(String) key IN';
DROP TABLE IF EXISTS t_nk;
CREATE TABLE t_nk (s Nullable(String)) ENGINE = MergeTree ORDER BY s SETTINGS allow_nullable_key = 1;
INSERT INTO t_nk VALUES ('a'), ('b'), ('c'), (NULL);
SELECT s FROM t_nk WHERE s IN (SELECT s FROM t_nk) ORDER BY s;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_nk WHERE s IN (SELECT s FROM t_nk)) WHERE explain ILIKE '%Condition:%in%set%';

-- IN error semantics must be preserved: a column-count mismatch still throws.
SELECT 'Column count mismatch still rejected';
SELECT 1 WHERE 1 IN (SELECT 1, 2); -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }

DROP TABLE t_str;
DROP TABLE t_fs;
DROP TABLE t_int;
DROP TABLE t_lc;
DROP TABLE t_date;
DROP TABLE t_nopk;
DROP TABLE t_np;
DROP TABLE t_ip;
DROP TABLE t_nk;
