-- Regression for issue flagged by `function_prop_fuzzer`: a non-constant `Tuple`
-- range mask in `hilbertEncode` / `mortonEncode` must compute per-row values, not
-- silently read row 0 for every row.

SELECT '----- hilbertEncode: simple mode (no Tuple) -----';
SELECT hilbertEncode(3, 4);
SELECT hilbertEncode(materialize(toUInt32(3)), materialize(toUInt32(4)));

SELECT '----- hilbertEncode: constant Tuple mask -----';
SELECT hilbertEncode((10, 6), 1024, 16);
SELECT hilbertEncode(tuple(2), materialize(toUInt32(128)));

SELECT '----- mortonEncode: simple mode (no Tuple) -----';
SELECT mortonEncode(1, 2, 3);
SELECT mortonEncode(materialize(toUInt32(1)), materialize(toUInt32(2)), materialize(toUInt32(3)));

SELECT '----- mortonEncode: constant Tuple mask -----';
SELECT mortonEncode((1, 2), 1024, 16);
SELECT mortonEncode(tuple(2), materialize(toUInt32(128)));

SELECT '----- hilbertEncode: multi-row non-constant Tuple mask is computed per row -----';
-- Row N has mask `tuple(N)`, value `3`, so the result is `3 << N`.
SELECT hilbertEncode(a, b) FROM (
    SELECT tuple(toUInt64(number)) AS a, toUInt16(3) AS b FROM numbers(3)
);
SELECT hilbertEncode(a, b, c) FROM (
    SELECT (toUInt64(number), toUInt64(number + 1)) AS a, toUInt32(1024) AS b, toUInt32(16) AS c FROM numbers(4)
);

SELECT '----- mortonEncode: multi-row non-constant Tuple mask is computed per row -----';
SELECT mortonEncode(a, b) FROM (
    SELECT tuple(toUInt64(number + 1)) AS a, toUInt16(3) AS b FROM numbers(3)
);
SELECT mortonEncode(a, b, c) FROM (
    SELECT (toUInt64(number + 1), toUInt64(number + 1)) AS a, toUInt32(1024) AS b, toUInt32(16) AS c FROM numbers(4)
);

SELECT '----- block-size independence: per-row computation is consistent across chunk sizes -----';
SET max_block_size = 1;

SELECT hilbertEncode(materialize(toUInt32(3)), materialize(toUInt32(4)));
SELECT hilbertEncode(tuple(2), materialize(toUInt32(128)));
SELECT mortonEncode(materialize(toUInt32(1)), materialize(toUInt32(2)), materialize(toUInt32(3)));
SELECT mortonEncode(tuple(2), materialize(toUInt32(128)));

SELECT hilbertEncode(a, b) FROM (
    SELECT tuple(toUInt64(number)) AS a, toUInt16(3) AS b FROM numbers(3)
);
SELECT mortonEncode(a, b) FROM (
    SELECT tuple(toUInt64(number + 1)) AS a, toUInt16(3) AS b FROM numbers(3)
);

SELECT hilbertEncode(a, b) FROM (
    SELECT tuple(toUInt64(2)) AS a, toUInt16(3) AS b FROM numbers(1)
);
SELECT mortonEncode(a, b) FROM (
    SELECT tuple(toUInt64(3)) AS a, toUInt16(3) AS b FROM numbers(1)
);

SELECT '----- per-row validation: out-of-range mask in any row is rejected -----';
SELECT hilbertEncode(a, b) FROM (
    SELECT tuple(toUInt64(33)) AS a, toUInt16(1) AS b FROM numbers(1)
); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT mortonEncode(a, b) FROM (
    SELECT tuple(toUInt64(9)) AS a, toUInt16(1) AS b FROM numbers(1)
); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT mortonEncode(a, b) FROM (
    SELECT tuple(toUInt64(0)) AS a, toUInt16(1) AS b FROM numbers(1)
); -- { serverError ARGUMENT_OUT_OF_BOUND }
