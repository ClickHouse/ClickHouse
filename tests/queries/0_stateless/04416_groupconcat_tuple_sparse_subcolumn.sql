-- Tags: no-random-settings, no-random-merge-tree-settings
-- groupConcat over a Tuple whose String element is stored sparse used to abort with
-- 'Bad cast from type DB::ColumnSparse to DB::ColumnString': the Aggregator only stripped
-- top-level sparsity, so a top-level-dense Tuple with sparse leaves reached serializeText.

DROP TABLE IF EXISTS t_gc_sparse;
DROP TABLE IF EXISTS t_gc_dense;

CREATE TABLE t_gc_sparse (id UInt64, t Tuple(a UInt64, s String)) ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.0, min_bytes_for_full_part_storage = 0;

CREATE TABLE t_gc_dense (id UInt64, t Tuple(a UInt64, s String)) ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 1.0;

INSERT INTO t_gc_sparse SELECT number, if(number % 4 = 0, (number, 'v' || toString(number)), (0, '')) FROM numbers(40);
INSERT INTO t_gc_dense  SELECT number, if(number % 4 = 0, (number, 'v' || toString(number)), (0, '')) FROM numbers(40);

-- The inner String must be sparse-serialized for this to exercise the bug.
SELECT DISTINCT subcolumns.serializations FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_gc_sparse' AND column = 't' AND active;

-- Must not crash, and must match the dense (never-sparse) result.
SELECT groupConcat(',')(t) FROM (SELECT t FROM t_gc_sparse ORDER BY id);
SELECT (SELECT groupConcat(',')(t) FROM (SELECT t FROM t_gc_sparse ORDER BY id))
     = (SELECT groupConcat(',')(t) FROM (SELECT t FROM t_gc_dense ORDER BY id));

-- Nested tuple and the limit variant also take the regular add() path.
SELECT groupConcat('|', 3)(t) FROM (SELECT t FROM t_gc_sparse ORDER BY id);
SELECT groupConcat(t) FROM (SELECT tuple(t) AS t FROM t_gc_sparse ORDER BY id LIMIT 4);

DROP TABLE t_gc_sparse;
DROP TABLE t_gc_dense;
