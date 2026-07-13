-- A Decimal size argument must be interpreted by its real (logical) value, not by the raw unscaled
-- representation. Previously arrayResize used `getInt`, which returns the underlying integer of a Decimal,
-- so e.g. arrayResize([1, 2, 3], 1.5::Decimal(2, 1)) wrongly resized to 15 elements.

SELECT arrayResize([1, 2, 3], 1.5::Decimal(2, 1));
SELECT arrayResize([1, 2, 3], 1.9::Decimal(2, 1));
SELECT arrayResize([1, 2, 3], 5::Decimal(4, 2));
SELECT arrayResize([1, 2, 3], 0.5::Decimal(2, 1));

-- A tiny fractional value rounds towards zero (this is the case the function fuzzer used to get stuck on).
SELECT arrayResize([1, 2, 3], -0.000000000000000000000006116852::Decimal(38, 30));

-- Negative size resizes from the right, same as for integers.
SELECT arrayResize([1, 2, 3], -2::Decimal(4, 2));

-- Consistent with Float and Integer sizes.
SELECT arrayResize([1, 2, 3], 1.9::Float64) = arrayResize([1, 2, 3], 1.9::Decimal(2, 1));
SELECT arrayResize([1, 2, 3], materialize(2)::Decimal(4, 2));
