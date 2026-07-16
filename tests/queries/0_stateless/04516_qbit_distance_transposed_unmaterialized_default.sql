-- The transposed QBit distance functions are rewritten by DistanceTransposedPartialReadsPass to read
-- individual bit-plane subcolumns (qb.1, qb.2, ...) of the QBit column instead of the whole column.
-- When the QBit column has a DEFAULT and is not yet materialized on a part, reading its subcolumns must
-- recompute the default from its source columns, exactly like a normal full-column read does. Previously
-- the source column of the default was not injected into the read set, so it was fed an empty array and
-- the implicit QBit CAST failed with "Array arguments must have size N for QBit conversion, got 0".
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/110634

SET allow_experimental_qbit_type = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_qbit_default;

-- Wide parts, so bit-plane subcolumns are separate streams that can be requested individually.
CREATE TABLE t_qbit_default (id UInt32, v Array(BFloat16))
    ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_qbit_default
    SELECT number, arrayMap(i -> toFloat32(i + number), range(8)) FROM numbers(8);

-- Add QBit columns with a DEFAULT but do NOT materialize them: the existing parts stay without qb/qi.
ALTER TABLE t_qbit_default
    ADD COLUMN qb QBit(BFloat16, 8, 8) DEFAULT CAST(v, 'Array(BFloat16)'),
    ADD COLUMN qi QBit(Int8, 8, 8) DEFAULT arrayMap(quantizeBFloat16ToInt8, CAST(v, 'Array(BFloat16)'));

-- The optimized subcolumn read (default) of the unmaterialized DEFAULT must agree with the unoptimized
-- full-column read. With the bug the optimized query threw SIZES_OF_ARRAYS_DONT_MATCH instead.
SELECT 'BFloat16 transposed on unmaterialized DEFAULT matches full-column read';
WITH arrayMap(i -> toFloat32(i), range(8)) AS ref
SELECT
    (SELECT arraySort(groupArray(round(cosineDistanceTransposed(qb, ref, 8), 4))) FROM t_qbit_default)
  = (SELECT arraySort(groupArray(round(cosineDistanceTransposed(qb, ref, 8), 4))) FROM t_qbit_default SETTINGS optimize_qbit_distance_function_reads = 0);

SELECT 'Int8 quantized transposed on unmaterialized DEFAULT matches full-column read';
WITH arrayMap(i -> toFloat32(i), range(8)) AS ref
SELECT
    (SELECT arraySort(groupArray(round(cosineDistanceTransposedQuantized(qi, ref, 8), 4))) FROM t_qbit_default)
  = (SELECT arraySort(groupArray(round(cosineDistanceTransposedQuantized(qi, ref, 8), 4))) FROM t_qbit_default SETTINGS optimize_qbit_distance_function_reads = 0);

-- After materializing the columns the transposed read must still give the same result.
ALTER TABLE t_qbit_default MATERIALIZE COLUMN qb SETTINGS mutations_sync = 2;
ALTER TABLE t_qbit_default MATERIALIZE COLUMN qi SETTINGS mutations_sync = 2;

SELECT 'BFloat16 transposed after MATERIALIZE COLUMN matches full-column read';
WITH arrayMap(i -> toFloat32(i), range(8)) AS ref
SELECT
    (SELECT arraySort(groupArray(round(cosineDistanceTransposed(qb, ref, 8), 4))) FROM t_qbit_default)
  = (SELECT arraySort(groupArray(round(cosineDistanceTransposed(qb, ref, 8), 4))) FROM t_qbit_default SETTINGS optimize_qbit_distance_function_reads = 0);

SELECT 'Int8 quantized transposed after MATERIALIZE COLUMN matches full-column read';
WITH arrayMap(i -> toFloat32(i), range(8)) AS ref
SELECT
    (SELECT arraySort(groupArray(round(cosineDistanceTransposedQuantized(qi, ref, 8), 4))) FROM t_qbit_default)
  = (SELECT arraySort(groupArray(round(cosineDistanceTransposedQuantized(qi, ref, 8), 4))) FROM t_qbit_default SETTINGS optimize_qbit_distance_function_reads = 0);

DROP TABLE t_qbit_default;
