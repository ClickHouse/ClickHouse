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

-- Binary-search path (>= 32 boundaries after NaN is dropped): NaN mixed into a large
-- boundary list must not break std::upper_bound's strict-weak-ordering. The list is a
-- constant literal (NaN + 0..39) so roundDown accepts the constant 2nd argument under
-- both the old and new analyzer.
SELECT roundDown(materialize(CAST(100 AS Float64)),
                 CAST([nan, 0., 1., 2., 3., 4., 5., 6., 7., 8., 9., 10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25., 26., 27., 28., 29., 30., 31., 32., 33., 34., 35., 36., 37., 38., 39.] AS Array(Float64))) AS r,
       r = (SELECT roundDown(b, CAST([nan, 0., 1., 2., 3., 4., 5., 6., 7., 8., 9., 10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25., 26., 27., 28., 29., 30., 31., 32., 33., 34., 35., 36., 37., 38., 39.] AS Array(Float64)))
            FROM system.one ARRAY JOIN CAST([100] AS Array(Float64)) AS b);

-- All boundaries are NaN: nothing to round to, result is NaN for every row (deterministic).
SELECT isNaN(roundDown(materialize(CAST(5 AS Float32)), CAST([nan, nan] AS Array(Float32))));
SELECT a, isNaN(roundDown(a, CAST([nan, nan] AS Array(Float32))))
FROM system.one ARRAY JOIN CAST([1, 2, 3] AS Array(Float32)) AS a
ORDER BY a;

-- NaN input is still propagated unchanged (regression guard for the original NaN-input fix).
SELECT isNaN(roundDown(materialize(CAST(nan AS Float32)), CAST([1, nan, 2] AS Array(Float32))));
