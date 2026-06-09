SELECT L1Norm(materialize([1.0, -2.0, 3.0, -4.0]::Array(Float32)));
SELECT L2Norm(materialize([1.0, 2.0, 2.0, 4.0]::Array(Float32)));
SELECT LinfNorm(materialize([1.0, -7.0, 3.0, -4.0]::Array(Float32)));
SELECT L2Distance(materialize([1.0, 2.0, 3.0, 4.0]::Array(Float32)), materialize([0.0, 1.0, 2.0, 3.0]::Array(Float32)));
SELECT L2Distance(materialize(range(16))::Array(Float32), materialize(range(16))::Array(Float32));
SELECT L2Distance(materialize(range(17))::Array(Float32), materialize(range(17))::Array(Float32));
WITH range(8)::Array(Float32) AS v SELECT L2Distance(v, materialize(range(8))::Array(Float32));
WITH range(9)::Array(Float32) AS v SELECT L2Distance(v, materialize(range(9))::Array(Float32));
WITH range(16)::Array(Float64) AS v SELECT L2Distance(v, materialize(range(16))::Array(Float64));
SELECT cosineDistance(materialize(arrayConcat([1.0], arrayResize([], 15, 0.0))::Array(Float32)), materialize(arrayConcat([0.0, 1.0], arrayResize([], 14, 0.0))::Array(Float32)));
SELECT cosineDistance(materialize(arrayConcat([1.0], arrayResize([], 16, 0.0))::Array(Float32)), materialize(arrayConcat([0.0, 1.0], arrayResize([], 15, 0.0))::Array(Float32)));
WITH arrayConcat([1.0], arrayResize([], 15, 0.0))::Array(Float32) AS v SELECT cosineDistance(v, materialize(arrayConcat([0.0, 1.0], arrayResize([], 14, 0.0))::Array(Float32)));
WITH arrayConcat([1.0], arrayResize([], 16, 0.0))::Array(Float32) AS v SELECT cosineDistance(v, materialize(arrayConcat([0.0, 1.0], arrayResize([], 15, 0.0))::Array(Float32)));
WITH arrayConcat([1.0], arrayResize([], 15, 0.0))::Array(Float64) AS v SELECT cosineDistance(v, materialize(arrayConcat([0.0, 1.0], arrayResize([], 14, 0.0))::Array(Float64)));
-- The shared vectorized loops in arrayDistance are instantiated for every distance kernel, so the exact-full-chunk
-- boundary must be checked for the other distance functions as well, not only L2Distance and cosineDistance.
-- Each pair below has a per-element difference of 1, so the expected values are exact integers.
-- Exact full chunk (length 16): vectorized path must process the last full chunk instead of the scalar tail.
WITH range(16)::Array(Float32) AS a, arrayMap(x -> x + 1, range(16))::Array(Float32) AS b SELECT L1Distance(materialize(a), materialize(b));
WITH range(16)::Array(Float32) AS a, arrayMap(x -> x + 1, range(16))::Array(Float32) AS b SELECT L2SquaredDistance(materialize(a), materialize(b));
WITH range(16)::Array(Float32) AS a, arrayMap(x -> x + 1, range(16))::Array(Float32) AS b SELECT LinfDistance(materialize(a), materialize(b));
WITH range(16)::Array(Float32) AS a, arrayMap(x -> x + 1, range(16))::Array(Float32) AS b SELECT LpDistance(materialize(a), materialize(b), 4);
-- One element past the full chunk (length 17): the scalar tail must still handle the leftover element.
WITH range(17)::Array(Float32) AS a, arrayMap(x -> x + 1, range(17))::Array(Float32) AS b SELECT L1Distance(materialize(a), materialize(b));
WITH range(17)::Array(Float32) AS a, arrayMap(x -> x + 1, range(17))::Array(Float32) AS b SELECT L2SquaredDistance(materialize(a), materialize(b));
WITH range(17)::Array(Float32) AS a, arrayMap(x -> x + 1, range(17))::Array(Float32) AS b SELECT LinfDistance(materialize(a), materialize(b));
-- Const left argument exercises the separate executeWithLeftArgConst loop (VEC_SIZE = 16).
WITH range(16)::Array(Float32) AS a SELECT L1Distance(a, materialize(arrayMap(x -> x + 1, range(16))::Array(Float32)));
WITH range(16)::Array(Float32) AS a SELECT LinfDistance(a, materialize(arrayMap(x -> x + 1, range(16))::Array(Float32)));
-- Float64 exact full chunk.
WITH range(16)::Array(Float64) AS a, arrayMap(x -> x + 1, range(16))::Array(Float64) AS b SELECT L1Distance(materialize(a), materialize(b));
