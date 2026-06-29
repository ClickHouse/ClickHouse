SET allow_experimental_bfloat16_type = 1;

-- Exact full-chunk boundaries for the AVX-512 const-left `arrayDistance` kernels changed here.
WITH range(16)::Array(Float32) AS a
SELECT round(pow(L2Distance(a, materialize(arrayMap(x -> x + 1, range(16))::Array(Float32))), 2));

WITH range(8)::Array(Float64) AS a
SELECT round(pow(L2Distance(a, materialize(arrayMap(x -> x + 1, range(8))::Array(Float64))), 2));

WITH arrayMap(x -> toBFloat16(x), range(32)) AS a
SELECT round(pow(L2Distance(a, materialize(arrayMap(x -> toBFloat16(x + 1), range(32)))), 2));

WITH arrayConcat([1.0], arrayResize([], 15, 0.0))::Array(Float32) AS a
SELECT cosineDistance(a, materialize(arrayConcat([0.0, 1.0], arrayResize([], 14, 0.0))::Array(Float32)));

WITH arrayConcat([1.0], arrayResize([], 7, 0.0))::Array(Float64) AS a
SELECT cosineDistance(a, materialize(arrayConcat([0.0, 1.0], arrayResize([], 6, 0.0))::Array(Float64)));

WITH arrayMap(x -> toBFloat16(if(x = 0, 1, 0)), range(32)) AS a
SELECT cosineDistance(a, materialize(arrayMap(x -> toBFloat16(if(x = 1, 1, 0)), range(32))));
