-- A merge can flip a source column to sparse serialization. That column then reaches the sink as
-- ColumnSparse, which used to hash row-wise while the same data materialized hashes column-wise, so
-- a retried INSERT ... SELECT stopped deduplicating even though its result never changed.

SET deduplicate_insert_select = 'enable_even_for_bad_queries';

DROP TABLE IF EXISTS t_dedup_sparse_src;
DROP TABLE IF EXISTS t_dedup_sparse_dst;

-- `s` is sparse at the top level, `t.s` is sparse nested in a Tuple. Both are String, because
-- fixed-width types hash identically row-wise and column-wise and would pass even unfixed.
CREATE TABLE t_dedup_sparse_src (id UInt64, s String, t Tuple(k UInt64, s String))
ENGINE = MergeTree ORDER BY id SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;
CREATE TABLE t_dedup_sparse_dst (id UInt64, s String, t Tuple(k UInt64, s String))
ENGINE = MergeTree ORDER BY id
SETTINGS non_replicated_deduplication_window = 100, ratio_of_defaults_for_sparse_serialization = 0.9;

INSERT INTO t_dedup_sparse_src VALUES (1, 'x', (1, 'y'));

INSERT INTO t_dedup_sparse_dst SELECT * FROM t_dedup_sparse_src WHERE id = 1;
INSERT INTO t_dedup_sparse_dst SELECT * FROM t_dedup_sparse_src WHERE id = 1;
SELECT 'retry before the merge', count() FROM t_dedup_sparse_dst;

-- Unrelated rows holding default values, every one of them filtered out by WHERE id = 1. The
-- merged part stores both String columns sparse.
INSERT INTO t_dedup_sparse_src SELECT number + 2, '', (0, '') FROM numbers(20);
OPTIMIZE TABLE t_dedup_sparse_src FINAL;

SELECT 'source is sparse', serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_dedup_sparse_src' AND column = 's' AND active;
SELECT 'tuple element is sparse', dumpColumnStructure(t) LIKE '%Sparse%'
FROM t_dedup_sparse_src LIMIT 1;
SELECT 'selected row unchanged', * FROM t_dedup_sparse_src WHERE id = 1;

INSERT INTO t_dedup_sparse_dst SELECT * FROM t_dedup_sparse_src WHERE id = 1;
SELECT 'retry after the merge', count() FROM t_dedup_sparse_dst;

-- The hash normalizes a copy, so sparse data still reaches disk sparse.
INSERT INTO t_dedup_sparse_dst SELECT * FROM t_dedup_sparse_src WHERE id = 2;
SELECT 'sparse rows stored sparse', countIf(serialization_kind = 'Sparse') FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_dedup_sparse_dst' AND column = 's' AND active;

DROP TABLE t_dedup_sparse_dst;
DROP TABLE t_dedup_sparse_src;
