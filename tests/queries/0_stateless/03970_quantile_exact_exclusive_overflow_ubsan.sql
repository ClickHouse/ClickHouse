-- Wide-range Int64 inputs: the interpolation delta overflows signed Int64 (UBSan).
SELECT quantileExactExclusive(0.5)(x) FROM (SELECT arrayJoin([CAST(-2147483647 AS Int64), CAST(9223372036854775807 AS Int64)]) AS x);
SELECT quantilesExactExclusive(0.5)(x) FROM (SELECT arrayJoin([CAST(-2147483647 AS Int64), CAST(9223372036854775807 AS Int64)]) AS x);
SELECT quantileExactInclusive(0.5)(x) FROM (SELECT arrayJoin([CAST(-2147483647 AS Int64), CAST(9223372036854775807 AS Int64)]) AS x);
SELECT quantilesExactInclusive(0.5)(x) FROM (SELECT arrayJoin([CAST(-2147483647 AS Int64), CAST(9223372036854775807 AS Int64)]) AS x);
-- High-magnitude integers (>= 2^53) with a small delta: the delta must be computed in an
-- exact integer intermediate, not by rounding each operand to Float64 first (which loses the
-- gap). For 2^54 and 2^54+5 at level 0.5 the interpolated value is 2^54+2.5 -> 18014398509481988.
SELECT quantileExactExclusive(0.5)(x) FROM (SELECT arrayJoin([CAST(18014398509481984 AS Int64), CAST(18014398509481989 AS Int64)]) AS x);
SELECT quantilesExactExclusive(0.5)(x) FROM (SELECT arrayJoin([CAST(18014398509481984 AS Int64), CAST(18014398509481989 AS Int64)]) AS x);
SELECT quantileExactInclusive(0.5)(x) FROM (SELECT arrayJoin([CAST(18014398509481984 AS Int64), CAST(18014398509481989 AS Int64)]) AS x);
SELECT quantilesExactInclusive(0.5)(x) FROM (SELECT arrayJoin([CAST(18014398509481984 AS Int64), CAST(18014398509481989 AS Int64)]) AS x);
