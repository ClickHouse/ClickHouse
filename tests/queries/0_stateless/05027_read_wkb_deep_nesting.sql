-- Multi* geometries nest into each other, and the element count limit does not bound the nesting
-- depth, because every level may hold a single element.
-- 25.8 has no generic `readWKB` function, only the type-specific ones, and no MultiPoint support.

SELECT readWKBMultiLineString(unhex(repeat('010500000001000000', 100000))); -- { serverError TOO_DEEP_RECURSION }
SELECT readWKBMultiPolygon(unhex(repeat('010600000001000000', 100000))); -- { serverError TOO_DEEP_RECURSION }

SELECT readWKBPoint(unhex('010100000000000000000000000000000000000000'));
