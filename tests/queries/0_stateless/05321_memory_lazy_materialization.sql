-- Lazy materialization for the `Memory` engine: for `ORDER BY ... LIMIT n`, the columns that are not
-- needed for sorting and filtering are read only for the `n` rows that survive the `LIMIT`.

SET query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10000;
SET max_threads = 4, max_block_size = 1000;

DROP TABLE IF EXISTS t_memory_lazy;
DROP TABLE IF EXISTS t_memory_lazy_compressed;

CREATE TABLE t_memory_lazy (k UInt64, v Int64, s String, a Array(UInt32), t Tuple(x UInt8, y String), n Nullable(String)) ENGINE = Memory;
CREATE TABLE t_memory_lazy_compressed (k UInt64, v Int64, s String, a Array(UInt32), t Tuple(x UInt8, y String), n Nullable(String)) ENGINE = Memory SETTINGS compress = 1;

INSERT INTO t_memory_lazy SELECT number, (number * 7919) % 100003, toString(number % 97), range(number % 5), (number % 7, toString(number)), if(number % 3 = 0, NULL, 'n' || toString(number)) FROM numbers(100000) SETTINGS max_block_size = 1000;
INSERT INTO t_memory_lazy_compressed SELECT * FROM t_memory_lazy;

SELECT '-- explain';
SELECT replaceRegexpOne(explain, '^[ │├└─]*', '') FROM (EXPLAIN actions = 1 SELECT * FROM t_memory_lazy ORDER BY v LIMIT 5) WHERE explain LIKE '%Lazily read columns%';
SELECT replaceRegexpOne(explain, '^[ │├└─]*', '') FROM (EXPLAIN actions = 1 SELECT * FROM t_memory_lazy WHERE s = '7' ORDER BY v LIMIT 5) WHERE explain LIKE '%Lazily read columns%';
SELECT replaceRegexpOne(explain, '^[ │├└─]*', '') FROM (EXPLAIN actions = 1 SELECT * FROM t_memory_lazy PREWHERE s = '7' ORDER BY v LIMIT 5) WHERE explain LIKE '%Lazily read columns%';
SELECT replaceRegexpOne(explain, '^[ │├└─]*', '') FROM (EXPLAIN actions = 1 SELECT k, v FROM t_memory_lazy ORDER BY v LIMIT 5) WHERE explain LIKE '%Lazily read columns%';
SELECT replaceRegexpOne(explain, '^[ │├└─]*', '') FROM (EXPLAIN actions = 1 SELECT * FROM t_memory_lazy ORDER BY v LIMIT 5 SETTINGS query_plan_optimize_lazy_materialization = 0) WHERE explain LIKE '%Lazily read columns%';

SELECT '-- results';
SELECT * FROM t_memory_lazy ORDER BY v LIMIT 5;
SELECT * FROM t_memory_lazy_compressed ORDER BY v DESC LIMIT 5;
SELECT * FROM t_memory_lazy ORDER BY v LIMIT 3 OFFSET 99990;
SELECT * FROM t_memory_lazy_compressed WHERE s = '7' ORDER BY v LIMIT 5;
SELECT * FROM t_memory_lazy PREWHERE s LIKE '%7%' WHERE length(a) = 2 ORDER BY v DESC LIMIT 5;
SELECT a, t.y, n FROM t_memory_lazy_compressed ORDER BY t.x DESC, k LIMIT 5;
SELECT k, length(n), arraySum(a) FROM t_memory_lazy ORDER BY k % 1000, k LIMIT 5;

SELECT '-- rows that come from many blocks, in an order unrelated to the order of the blocks';
SELECT sum(cityHash64(*)) FROM (SELECT * FROM t_memory_lazy ORDER BY cityHash64(k) LIMIT 1000);
SELECT sum(cityHash64(*)) FROM (SELECT * FROM t_memory_lazy ORDER BY cityHash64(k) LIMIT 1000 SETTINGS query_plan_optimize_lazy_materialization = 0);
SELECT sum(cityHash64(*)) FROM (SELECT * FROM t_memory_lazy_compressed WHERE v % 3 = 1 ORDER BY v DESC, k LIMIT 1000);
SELECT sum(cityHash64(*)) FROM (SELECT * FROM t_memory_lazy_compressed WHERE v % 3 = 1 ORDER BY v DESC, k LIMIT 1000 SETTINGS query_plan_optimize_lazy_materialization = 0);

SELECT '-- every column is deferred, the main branch reads only the global row index';
-- The order of the blocks depends on randomized insert settings, so check that the lazily read
-- columns belong to the same row and match the result without lazy materialization.
SELECT k % 97 = toUInt64(s), n IS NULL OR n = 'n' || toString(k) FROM (SELECT k, s, n FROM t_memory_lazy ORDER BY rowNumberInAllBlocks() LIMIT 3) SETTINGS max_threads = 1;
SELECT (SELECT groupArray((k, s, n)) FROM (SELECT k, s, n FROM t_memory_lazy ORDER BY rowNumberInAllBlocks() LIMIT 3))
    = (SELECT groupArray((k, s, n)) FROM (SELECT k, s, n FROM t_memory_lazy ORDER BY rowNumberInAllBlocks() LIMIT 3 SETTINGS query_plan_optimize_lazy_materialization = 0))
SETTINGS max_threads = 1;
SELECT s, n FROM t_memory_lazy_compressed ORDER BY rand() LIMIT 1000 FORMAT Null;

SELECT '-- a row policy over a column that is not otherwise needed before the LIMIT';
DROP ROW POLICY IF EXISTS 05321_memory_lazy_policy ON t_memory_lazy_compressed;
CREATE ROW POLICY 05321_memory_lazy_policy ON t_memory_lazy_compressed USING length(a) = 3 TO ALL;
SELECT * FROM t_memory_lazy_compressed ORDER BY v LIMIT 3;
SELECT * FROM t_memory_lazy_compressed PREWHERE s = '7' ORDER BY v LIMIT 3;
DROP ROW POLICY 05321_memory_lazy_policy ON t_memory_lazy_compressed;

SELECT '-- a column added after the blocks were inserted';
ALTER TABLE t_memory_lazy ADD COLUMN m String;
INSERT INTO t_memory_lazy (k, v, s, m) VALUES (100000, -1, 'x', 'new'), (100001, -2, 'y', 'newer');
SELECT k, v, s, m FROM t_memory_lazy ORDER BY v LIMIT 4;

SELECT '-- an empty table';
TRUNCATE TABLE t_memory_lazy;
SELECT * FROM t_memory_lazy ORDER BY v LIMIT 5;

DROP TABLE t_memory_lazy;
DROP TABLE t_memory_lazy_compressed;
