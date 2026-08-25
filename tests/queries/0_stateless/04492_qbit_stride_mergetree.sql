-- A strided QBit must round-trip through MergeTree storage (separate streams per stride group) across a merge,
-- and the partial-dimension distance search must work on the merged part.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS qbit_mt;
CREATE TABLE qbit_mt (id UInt32, vec QBit(Float32, 16, 8)) ENGINE = MergeTree ORDER BY id;

-- Two parts, then merge them, so the per-stride-group streams are written, merged and read back.
INSERT INTO qbit_mt VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
INSERT INTO qbit_mt VALUES (2, [16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]);
OPTIMIZE TABLE qbit_mt FINAL;

SELECT 'Round-trip after merge';
SELECT id, vec FROM qbit_mt ORDER BY id;

SELECT 'Partial-dimension distance over the first 8 dims';
WITH [toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS ref SELECT id, round(L2DistanceTransposed(vec, ref, 32, 8), 4) FROM qbit_mt ORDER BY id;

SELECT 'Full-dimension distance';
WITH arrayMap(i -> toFloat32(i), range(16)) AS ref SELECT id, round(L2DistanceTransposed(vec, ref, 32), 4) FROM qbit_mt ORDER BY id;

DROP TABLE qbit_mt;
