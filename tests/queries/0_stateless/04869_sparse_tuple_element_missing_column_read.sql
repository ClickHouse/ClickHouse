-- Reading a column whose Tuple element is stored sparse together with a column missing from the
-- part must not trip the type/column structure check in `collectOffsetsColumns`:
-- the serialization is built from the column's own serialization info, so a sparse element at any
-- nesting level is a legitimate pairing of the declared type with a `ColumnSparse`.

DROP TABLE IF EXISTS t_sparse_tuple_missing;

CREATE TABLE t_sparse_tuple_missing (k UInt64, c1 Tuple(Nullable(Int32))) ENGINE = MergeTree ORDER BY k
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5, min_bytes_for_wide_part = 0, nullable_serialization_version = 'allow_sparse';

INSERT INTO t_sparse_tuple_missing SELECT number, tuple(if(number = 0, 42, NULL)) FROM numbers(142);

-- The column `arr` does not exist in the already written part, so reads go through `fillMissingColumns`.
ALTER TABLE t_sparse_tuple_missing ADD COLUMN arr Array(UInt64);

SELECT count(), sum(c1.1 IS NULL), sum(length(arr)) FROM (SELECT k, c1, arr FROM t_sparse_tuple_missing);

DROP TABLE t_sparse_tuple_missing;
