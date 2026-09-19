-- exp10 is vectorized and approximate (~1e-12 relative), so compare against the exactly parsed literal 1eN.
SELECT n, f = e OR abs(f / e - 1) < 1e-11 FROM (SELECT toInt64(number) - 500 AS n, exp10(n) AS f, toFloat64('1e' || toString(n)) AS e FROM system.numbers LIMIT 1000);
SELECT exp10(nan);
