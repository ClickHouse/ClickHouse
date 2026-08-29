-- `exp10` is vectorized in builds with FastOps and approximate (~1e-12 relative), so it is
-- compared with a tolerance instead of being printed; the exactly parsed literal `1eN` stands in for it
-- in the string comparison with `intExp10`.
SELECT
    exp2(number) AS e2d,
    intExp2(number) AS e2i,
    toUInt64(e2d) = e2i AS e2eq,
    toFloat64('1e' || toString(number)) AS e10d,
    abs(exp10(number) / e10d - 1) < 1e-11 AS e10ok,
    intExp10(number) AS e10i,
    toString(e10d) = toString(e10i) AS e10eq
FROM system.numbers LIMIT 64;
