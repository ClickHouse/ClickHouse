-- Reading a subcolumn of a column that is missing in a part must evaluate the column's DEFAULT
-- expression (reading its source columns from the part) instead of substituting type defaults.
-- https://github.com/ClickHouse/ClickHouse/issues/110634

SET allow_experimental_qbit_type = 1;
SET optimize_qbit_distance_function_reads = 1;

DROP TABLE IF EXISTS t_qbit_subcolumn_default;

CREATE TABLE t_qbit_subcolumn_default (id UInt32, v Array(BFloat16))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

-- Rows 0..9 point in the same direction as the reference vector (row 0), rows 10..19 are orthogonal to it.
INSERT INTO t_qbit_subcolumn_default
    SELECT number, arrayMap(i -> toFloat32(if(number < 10, i < 16, i >= 16)), range(32))
    FROM numbers(20);

-- The parts written above do not contain the new columns physically.
ALTER TABLE t_qbit_subcolumn_default
    ADD COLUMN qb QBit(BFloat16, 32, 32) DEFAULT CAST(v, 'Array(BFloat16)'),
    ADD COLUMN qi QBit(Int8, 32, 32) DEFAULT arrayMap(x -> quantizeBFloat16ToInt8(x), CAST(v, 'Array(BFloat16)'));

SELECT 'before MATERIALIZE COLUMN';

WITH (SELECT CAST(qb, 'Array(Float32)') FROM t_qbit_subcolumn_default WHERE id = 0) AS r
SELECT count() FROM t_qbit_subcolumn_default WHERE cosineDistanceTransposed(qb, r, 16, 32) < 0.5;

WITH (SELECT CAST(qi, 'Array(Float32)') FROM t_qbit_subcolumn_default WHERE id = 0) AS r
SELECT count() FROM t_qbit_subcolumn_default WHERE cosineDistanceTransposedQuantized(qi, r, 8, 32) < 0.5;

-- The unoptimized path (full column read) must agree with the subcolumn read path.
WITH (SELECT CAST(qb, 'Array(Float32)') FROM t_qbit_subcolumn_default WHERE id = 0) AS r
SELECT count() FROM t_qbit_subcolumn_default WHERE cosineDistanceTransposed(qb, r, 16, 32) < 0.5
SETTINGS optimize_qbit_distance_function_reads = 0;

SELECT 'during MATERIALIZE COLUMN';

-- Keep the mutation pending to test the window while it is in progress.
SYSTEM STOP MERGES t_qbit_subcolumn_default;

ALTER TABLE t_qbit_subcolumn_default
    MATERIALIZE COLUMN qb, MATERIALIZE COLUMN qi
    SETTINGS mutations_sync = 0;

WITH (SELECT CAST(qb, 'Array(Float32)') FROM t_qbit_subcolumn_default WHERE id = 0) AS r
SELECT count() FROM t_qbit_subcolumn_default WHERE cosineDistanceTransposed(qb, r, 16, 32) < 0.5;

WITH (SELECT CAST(qi, 'Array(Float32)') FROM t_qbit_subcolumn_default WHERE id = 0) AS r
SELECT count() FROM t_qbit_subcolumn_default WHERE cosineDistanceTransposedQuantized(qi, r, 8, 32) < 0.5;

SYSTEM START MERGES t_qbit_subcolumn_default;

DROP TABLE t_qbit_subcolumn_default;

-- The same bug returned silently wrong values (type defaults) for subcolumns of ordinary columns.
SELECT 'Tuple subcolumn of a column with DEFAULT';

DROP TABLE IF EXISTS t_tuple_subcolumn_default;

CREATE TABLE t_tuple_subcolumn_default (id UInt32, x UInt32) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_tuple_subcolumn_default SELECT number, number + 100 FROM numbers(5);

ALTER TABLE t_tuple_subcolumn_default ADD COLUMN tup Tuple(a UInt32, b UInt32) DEFAULT (x, x * 2);

SELECT tup.a, tup.b FROM t_tuple_subcolumn_default ORDER BY id;

DROP TABLE t_tuple_subcolumn_default;
