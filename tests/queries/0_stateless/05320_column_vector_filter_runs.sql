-- UInt128 keeps both filter overloads on the generic ColumnVector path.
SET max_block_size = 256;
SET max_threads = 1;
SET enable_optimize_predicate_expression = 0;

WITH
    arrayConcat(range(4, 12), range(20, 36), range(48, 56)) AS selected_offsets,
    arrayConcat(
        selected_offsets,
        arrayMap(x -> x + 64, selected_offsets),
        arrayMap(x -> x + 128, selected_offsets),
        arrayMap(x -> x + 192, selected_offsets)) AS expected
SELECT groupArray(toUInt64(value)) = expected
FROM
(
    SELECT toUInt128(number) AS value
    FROM numbers(256)
    WHERE (value % 64 BETWEEN 4 AND 11)
       OR (value % 64 BETWEEN 20 AND 35)
       OR (value % 64 BETWEEN 48 AND 55)
);

DROP TABLE IF EXISTS t_05320;
CREATE TABLE t_05320 (k UInt64, payload UInt128) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_05320 SELECT number, toUInt128(number) FROM numbers(65536);

WITH
    arrayConcat(range(4, 12), range(20, 36), range(48, 56)) AS selected_offsets,
    arrayFlatten(arrayMap(
        block -> arrayMap(offset -> block + offset, selected_offsets),
        range(0, 65536, 64))) AS expected
SELECT groupArray(toUInt64(payload)) = expected
FROM
(
    SELECT k, payload
    FROM t_05320
    PREWHERE (k % 64 BETWEEN 4 AND 11)
          OR (k % 64 BETWEEN 20 AND 35)
          OR (k % 64 BETWEEN 48 AND 55)
);

DROP TABLE t_05320;
