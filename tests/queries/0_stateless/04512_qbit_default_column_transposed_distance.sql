-- https://github.com/ClickHouse/ClickHouse/issues/110634
-- `cosineDistanceTransposed` / `cosineDistanceTransposedQuantized` rewrite their `QBit` argument into a read of the
-- individual bit-plane subcolumns (e.g. `qb.1`, `qb.2`, ...), see `DistanceTransposedPartialReadsPass`. When the
-- `QBit` column is not yet physically materialized in a part (its value is computed from a `DEFAULT` expression),
-- the part-level default injection must still recompute it from the real source column, exactly like a plain
-- (non-transposed) read of the column does.

SET allow_experimental_qbit_type = 1;

DROP TABLE IF EXISTS t_qbit_default_transposed;

CREATE TABLE t_qbit_default_transposed (id UInt32, v Array(BFloat16)) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_qbit_default_transposed VALUES (0, [0, 1, 2, 3]), (1, [1, 2, 3, 4]), (2, [2, 3, 4, 5]);

-- Keep the column permanently un-materialized in the existing part: a background merge would otherwise
-- materialize `qb` and mask the bug.
SYSTEM STOP MERGES t_qbit_default_transposed;

ALTER TABLE t_qbit_default_transposed
    ADD COLUMN qb QBit(BFloat16, 4) DEFAULT CAST(v, 'Array(BFloat16)'),
    ADD COLUMN qi QBit(Int8, 4) DEFAULT arrayMap(x -> quantizeBFloat16ToInt8(x), CAST(v, 'Array(BFloat16)'));

SELECT 'qb, qi are not materialized in the part yet';
SELECT name, column FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_qbit_default_transposed' AND active AND column IN ('qb', 'qi');

SELECT 'A plain read recomputes the DEFAULT correctly';
SELECT id, CAST(qb, 'Array(BFloat16)'), CAST(qi, 'Array(Int8)') FROM t_qbit_default_transposed ORDER BY id;

SELECT 'cosineDistanceTransposed must recompute the DEFAULT too, instead of reading an empty source column';
WITH (SELECT CAST(qb, 'Array(Float32)') FROM t_qbit_default_transposed WHERE id = 0) AS r
SELECT id, round(cosineDistanceTransposed(qb, r, 16, 4), 4) FROM t_qbit_default_transposed ORDER BY id;

SELECT 'cosineDistanceTransposedQuantized must recompute the DEFAULT too, instead of reading an empty source column';
WITH (SELECT CAST(qi, 'Array(Float32)') FROM t_qbit_default_transposed WHERE id = 0) AS r
SELECT id, round(cosineDistanceTransposedQuantized(qi, r, 8, 4), 4) FROM t_qbit_default_transposed ORDER BY id;

DROP TABLE t_qbit_default_transposed;
