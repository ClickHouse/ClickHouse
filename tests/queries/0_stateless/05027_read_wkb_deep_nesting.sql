-- Multi* geometries nest into each other, and the element count limit does not bound the nesting
-- depth, because every level may hold a single element.

SELECT readWKB(unhex(repeat('010500000001000000', 100000))); -- { serverError TOO_DEEP_RECURSION }
SELECT readWKB(unhex(repeat('010600000001000000', 100000))); -- { serverError TOO_DEEP_RECURSION }

SELECT readWKB(unhex('010100000000000000000000000000000000000000'));
