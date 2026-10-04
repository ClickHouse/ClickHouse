-- TopN dynamic filtering (`use_top_k_dynamic_filtering`) for the `Memory` engine: the rows that cannot
-- enter the top-K heap are dropped inside the source. The result must be the same as without it.

SET use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_variable_length_types = 1, query_plan_max_limit_for_top_k_optimization = 1000;
SET max_threads = 1, max_block_size = 1000;

DROP TABLE IF EXISTS t_memory_top_k;
DROP TABLE IF EXISTS t_memory_top_k_compressed;

CREATE TABLE t_memory_top_k (k UInt64, v Nullable(Int64), s String, p String) ENGINE = Memory;
CREATE TABLE t_memory_top_k_compressed (k UInt64, v Nullable(Int64), s String, p String) ENGINE = Memory SETTINGS compress = 1;

INSERT INTO t_memory_top_k SELECT number, if(number % 11 = 0, NULL, (number * 7919) % 100003), toString(number % 97), repeat('x', number % 10) FROM numbers(100000) SETTINGS max_block_size = 1000;
INSERT INTO t_memory_top_k_compressed SELECT * FROM t_memory_top_k;

SELECT '-- explain';
SELECT replaceRegexpOne(explain, '^[ │├└─]*', '') FROM (EXPLAIN actions = 1 SELECT * FROM t_memory_top_k ORDER BY k LIMIT 5) WHERE explain LIKE '%TopN filter%';
SELECT replaceRegexpOne(explain, '^[ │├└─]*', '') FROM (EXPLAIN actions = 1 SELECT * FROM t_memory_top_k ORDER BY k LIMIT 5 SETTINGS use_top_k_dynamic_filtering = 0) WHERE explain LIKE '%TopN filter%';
SELECT replaceRegexpOne(explain, '^[ │├└─]*', '') FROM (EXPLAIN actions = 1 SELECT * FROM t_memory_top_k WHERE s LIKE '%7%' ORDER BY v DESC LIMIT 5) WHERE explain LIKE '%TopN filter%';

SELECT '-- ascending';
SELECT k, v, s FROM t_memory_top_k ORDER BY v, k LIMIT 5;
SELECT k, v, s FROM t_memory_top_k_compressed ORDER BY v, k LIMIT 5;

SELECT '-- descending';
SELECT k, v, s FROM t_memory_top_k ORDER BY v DESC, k LIMIT 5;
SELECT k, v, s FROM t_memory_top_k_compressed ORDER BY v DESC, k LIMIT 5;

SELECT '-- nulls first';
SELECT k, v, s FROM t_memory_top_k ORDER BY v DESC NULLS FIRST, k LIMIT 3 OFFSET 9089;
SELECT k, v, s FROM t_memory_top_k_compressed ORDER BY v ASC NULLS FIRST, k LIMIT 3 OFFSET 9089;

SELECT '-- string key';
SELECT k, s FROM t_memory_top_k ORDER BY s DESC, k LIMIT 3;
SELECT k, s FROM t_memory_top_k_compressed ORDER BY s, k DESC LIMIT 3;

SELECT '-- with WHERE and PREWHERE';
SELECT k, v, s FROM t_memory_top_k WHERE s LIKE '%7%' ORDER BY v DESC LIMIT 3;
SELECT k, v, s FROM t_memory_top_k_compressed PREWHERE s LIKE '%7%' WHERE length(p) = 3 ORDER BY v LIMIT 3;
SELECT k, v, s FROM t_memory_top_k_compressed PREWHERE s = '7' ORDER BY k DESC LIMIT 3;

SELECT '-- the filter must not change the rows a block-dependent condition sees';
SELECT k FROM t_memory_top_k WHERE rowNumberInBlock() = 999 ORDER BY k DESC LIMIT 3;
SELECT k FROM t_memory_top_k PREWHERE rowNumberInBlock() = 999 ORDER BY k DESC LIMIT 3;

SELECT '-- a column added after the blocks were inserted';
ALTER TABLE t_memory_top_k ADD COLUMN n UInt64;
INSERT INTO t_memory_top_k (k, v, s, p, n) VALUES (100000, 1, 'a', 'b', 7), (100001, 1, 'a', 'b', 100);
SELECT k, n FROM t_memory_top_k ORDER BY n, k LIMIT 3;
SELECT k, n FROM t_memory_top_k ORDER BY n DESC, k LIMIT 3;

DROP TABLE t_memory_top_k;
DROP TABLE t_memory_top_k_compressed;
