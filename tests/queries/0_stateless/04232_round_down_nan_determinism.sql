-- `roundDown(nan, [...])` used to return different values in scalar and vector paths.
-- Both paths walked the sorted boundaries using comparisons that all evaluate to false
-- for NaN: the linear-search path (boundaries < 32) carried over the previous row's
-- iterator position, and the binary-search path called `std::upper_bound` whose
-- strict-weak-ordering contract is violated by NaN. NaN now propagates through both
-- paths unchanged.

-- Small boundary list (linear-search path).
SELECT 'f32 scalar', roundDown(CAST(nan AS Float32), [3::Int8, -4]);
SELECT 'f32 vector', roundDown(materialize(CAST(nan AS Float32)), [3::Int8, -4]);
SELECT 'f64 scalar', roundDown(CAST(nan AS Float64), [3::Int8, -4]);
SELECT 'f64 vector', roundDown(materialize(CAST(nan AS Float64)), [3::Int8, -4]);

-- Mixed NaN / real values, exact stress test pattern: each NaN row must give NaN,
-- each finite row must give the matching boundary independently of preceding rows.
SELECT n, isNaN(r), r FROM (
    SELECT number AS n,
           roundDown(arrayElement(
               CAST([-2.749135e38, 1.009e-41, nan, 2.531572e-36, nan, 0, -100] AS Array(Float32)),
               number + 1), [3::Int8, -4]) AS r
    FROM numbers(7)
) ORDER BY n;

-- Large boundary list (>= 32) exercises the binary-search path.
SELECT n, isNaN(r) FROM (
    SELECT number AS n,
           roundDown(arrayElement(CAST([nan, 1.5, nan] AS Array(Float64)), number + 1),
                     range(40)) AS r
    FROM numbers(3)
) ORDER BY n;
