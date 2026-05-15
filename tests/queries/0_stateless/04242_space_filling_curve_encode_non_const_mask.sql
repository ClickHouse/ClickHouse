-- Regression for a silent wrong-result bug in `hilbertEncode` and `mortonEncode`:
-- when the first argument was a non-constant `Tuple` column, the implementations
-- read the range mask from row 0 and used it for every row, producing wrong results.
-- Detected by `function_prop_fuzzer` as a determinism violation:
-- single-row vs multi-row execution of the same input row produced different values.
--
-- After the fix, a non-constant `Tuple` first argument is accepted: the encoder reads
-- the mask values of each row independently and produces the same output that a single-
-- row query of the same logical row would produce. Result is block-size independent.

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
-- Before the fix the implementation silently used row 0's mask values to drive
-- the bit-shift for every row, producing wrong results (the fuzzer flagged this as
-- single-row vs multi-row determinism violation). After the fix each row uses its
-- own mask: row N has mask `tuple(N)`, value `3`, so the result is `3 << N`.
SELECT hilbertEncode(a, b) FROM (
    SELECT tuple(toUInt64(number)) AS a, toUInt16(3) AS b FROM numbers(3)
);
-- Two-dimensional mask varying per row: row N has mask `(N, N+1)`, b=1024, c=16.
-- Per-row shifts: b << N and c << (N+1) feed into the 2D Hilbert encoder.
SELECT hilbertEncode(a, b, c) FROM (
    SELECT (toUInt64(number), toUInt64(number + 1)) AS a, toUInt32(1024) AS b, toUInt32(16) AS c FROM numbers(4)
);

SELECT '----- mortonEncode: multi-row non-constant Tuple mask is computed per row -----';
-- Row N has mask `tuple(N+1)` (so ratios are 1, 2, 3), value `3`.
-- Per-row results: ratio=1 → 3; ratio=2 → MortonND_2D.Encode(0, 3); ratio=3 → MortonND_3D.Encode(0, 0, 3).
SELECT mortonEncode(a, b) FROM (
    SELECT tuple(toUInt64(number + 1)) AS a, toUInt16(3) AS b FROM numbers(3)
);
SELECT mortonEncode(a, b, c) FROM (
    SELECT (toUInt64(number + 1), toUInt64(number + 1)) AS a, toUInt32(1024) AS b, toUInt32(16) AS c FROM numbers(4)
);

SELECT '----- block-size independence: per-row computation is consistent across chunk sizes -----';
-- Previously, with `SET max_block_size = 1` the same expression silently produced
-- different values because each chunk was 1 row and row 0 of a 1-row chunk is the
-- correct row. With the default block size all rows used row 0 of the multi-row
-- chunk, producing wrong results. After the fix the output is identical regardless
-- of `max_block_size`.
SET max_block_size = 1;

-- Simple-mode and constant-mask paths continue to work with block size 1.
SELECT hilbertEncode(materialize(toUInt32(3)), materialize(toUInt32(4)));
SELECT hilbertEncode(tuple(2), materialize(toUInt32(128)));
SELECT mortonEncode(materialize(toUInt32(1)), materialize(toUInt32(2)), materialize(toUInt32(3)));
SELECT mortonEncode(tuple(2), materialize(toUInt32(128)));

-- Non-constant Tuple masks now produce the same per-row values as with the default block size.
SELECT hilbertEncode(a, b) FROM (
    SELECT tuple(toUInt64(number)) AS a, toUInt16(3) AS b FROM numbers(3)
);
SELECT mortonEncode(a, b) FROM (
    SELECT tuple(toUInt64(number + 1)) AS a, toUInt16(3) AS b FROM numbers(3)
);

-- And the result for a naturally single-row source matches a single row of the multi-row source.
SELECT hilbertEncode(a, b) FROM (
    SELECT tuple(toUInt64(2)) AS a, toUInt16(3) AS b FROM numbers(1)
);
SELECT mortonEncode(a, b) FROM (
    SELECT tuple(toUInt64(3)) AS a, toUInt16(3) AS b FROM numbers(1)
);

SELECT '----- per-row validation: out-of-range mask in any row is rejected -----';
-- For hilbert the ratio must be 0-32 and for morton must be 1-8. Validation now
-- runs per row for non-constant masks, so an out-of-range value in any row throws.
-- The test uses single-row sources so the validation fires before any output is
-- streamed; mid-sequence bad rows would emit the preceding rows in 1-row chunks
-- (with `SET max_block_size = 1` above), which would make the test output depend
-- on chunk size — orthogonal to what we want to assert here.
SELECT hilbertEncode(a, b) FROM (
    SELECT tuple(toUInt64(33)) AS a, toUInt16(1) AS b FROM numbers(1)
); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT mortonEncode(a, b) FROM (
    SELECT tuple(toUInt64(9)) AS a, toUInt16(1) AS b FROM numbers(1)
); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT mortonEncode(a, b) FROM (
    SELECT tuple(toUInt64(0)) AS a, toUInt16(1) AS b FROM numbers(1)
); -- { serverError ARGUMENT_OUT_OF_BOUND }
