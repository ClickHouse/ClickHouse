-- Test arrayNorm functions over Array(BFloat16): values widen to Float32 in the vectorized kernel
SET allow_experimental_bfloat16_type = 1;
DROP TABLE IF EXISTS t_array_norm_bf16;
CREATE TABLE t_array_norm_bf16 (id UInt64, v Array(BFloat16)) ENGINE = Memory;
-- id 1,2,3: short arrays (tail-only path); id 4: 40 elements (unrolled 16x loop + tail)
INSERT INTO t_array_norm_bf16 VALUES (1, [3, 4]), (2, [2]), (3, [3, 3, 3]), (4, arrayMap(x -> toBFloat16(x), range(1, 41)));

SELECT toTypeName(L2Norm([1, 2]::Array(BFloat16)));
SELECT id, L1Norm(v), L2Norm(v), L2SquaredNorm(v), LpNorm(v, 2.7), LinfNorm(v) FROM t_array_norm_bf16 ORDER BY id;

DROP TABLE t_array_norm_bf16;
