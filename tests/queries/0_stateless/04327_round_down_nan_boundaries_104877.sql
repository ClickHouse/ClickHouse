-- roundDown must give the same result for a value whether evaluated alone or inside a batch,
-- even when the boundaries array contains NaN.

-- Exact reproducer from the issue: single-row vs multi-row must agree for the same input.
SELECT roundDown(materialize(CAST(1 AS Int32)), CAST([3.2508048e-38, nan, 2.854158e-38, nan] AS Array(Float32))) =
    (SELECT roundDown(a, CAST([3.2508048e-38, nan, 2.854158e-38, nan] AS Array(Float32)))
     FROM system.one ARRAY JOIN CAST([1] AS Array(Int32)) AS a);

-- Linear-search path (< 32 boundaries): the batched result for each value must equal the
-- single-row result for that same value. The function reference below has no NaN, so every
-- row gets a finite boundary regardless of its neighbours.
SELECT a, roundDown(a, CAST([10, nan, 20, 5] AS Array(Float32))) AS r
FROM system.one ARRAY JOIN CAST([1, 7, 15, 100, 3] AS Array(Float32)) AS a
ORDER BY a;

SELECT 'single-row';
SELECT roundDown(materialize(CAST(1 AS Float32)), CAST([10, nan, 20, 5] AS Array(Float32)));
SELECT roundDown(materialize(CAST(3 AS Float32)), CAST([10, nan, 20, 5] AS Array(Float32)));
SELECT roundDown(materialize(CAST(7 AS Float32)), CAST([10, nan, 20, 5] AS Array(Float32)));
SELECT roundDown(materialize(CAST(15 AS Float32)), CAST([10, nan, 20, 5] AS Array(Float32)));
SELECT roundDown(materialize(CAST(100 AS Float32)), CAST([10, nan, 20, 5] AS Array(Float32)));

-- Binary-search path (>= 32 boundaries): NaN mixed into a large boundary list must not
-- break std::upper_bound's strict-weak-ordering. range(40) gives boundaries 0..39.
SELECT roundDown(materialize(CAST(100 AS Float64)),
                 arrayConcat(CAST([nan] AS Array(Float64)), arrayMap(x -> toFloat64(x), range(40)))) AS r,
       r = (SELECT roundDown(b, arrayConcat(CAST([nan] AS Array(Float64)), arrayMap(x -> toFloat64(x), range(40))))
            FROM system.one ARRAY JOIN CAST([100] AS Array(Float64)) AS b);

-- All boundaries are NaN: nothing to round to, result is NaN for every row (deterministic).
SELECT isNaN(roundDown(materialize(CAST(5 AS Float32)), CAST([nan, nan] AS Array(Float32))));
SELECT a, isNaN(roundDown(a, CAST([nan, nan] AS Array(Float32))))
FROM system.one ARRAY JOIN CAST([1, 2, 3] AS Array(Float32)) AS a
ORDER BY a;

-- NaN input is still propagated unchanged (regression guard for the original NaN-input fix).
SELECT isNaN(roundDown(materialize(CAST(nan AS Float32)), CAST([1, nan, 2] AS Array(Float32))));
