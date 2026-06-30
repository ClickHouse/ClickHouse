SET allow_experimental_nullable_array_type = 1;

-- Dynamic column with a runtime variant containing Nullable(Array(...))
-- should not alias substreams across different variant alternatives.
-- This test exercises MergeTree substream naming for Dynamic runtime variants.
CREATE TABLE t_dynamic (id UInt8, d Dynamic) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_dynamic VALUES (1, CAST([1, 2] AS Nullable(Array(UInt8)))), (2, CAST([3] AS Nullable(Array(UInt16))));

-- Force a merge to exercise substream naming
OPTIMIZE TABLE t_dynamic FINAL;

-- Verify data integrity after merge
SELECT throwIf(isNull(d) OR d != CAST([1, 2] AS Nullable(Array(UInt8))), 'Row 1 corrupted after merge') FROM t_dynamic WHERE id = 1;
SELECT throwIf(isNull(d) OR d != CAST([3] AS Nullable(Array(UInt16))), 'Row 2 corrupted after merge') FROM t_dynamic WHERE id = 2;

DROP TABLE t_dynamic;
