-- Coverage for ColumnSparse code paths via ratio_of_defaults_for_sparse_serialization.
-- When the ratio of default (zero/empty) values exceeds the threshold, MergeTree
-- stores the column in sparse format, exercising ColumnSparse read/write paths.

DROP TABLE IF EXISTS t_sparse;

CREATE TABLE t_sparse
(
    id   UInt32,
    val  UInt64
)
ENGINE = MergeTree
ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.1;

-- Insert a batch where most values are 0 (default) to trigger sparse encoding.
INSERT INTO t_sparse
SELECT
    number AS id,
    if(number % 20 = 0, number, 0) AS val
FROM numbers(200);

-- Force a merge so sparse serialization is applied.
OPTIMIZE TABLE t_sparse FINAL;

-- Basic read covering sparse deserialization.
SELECT count(), sum(val) FROM t_sparse;

-- Filter on the sparse column to exercise predicate pushdown on sparse data.
SELECT id, val FROM t_sparse WHERE val != 0 ORDER BY id;

DROP TABLE t_sparse;
