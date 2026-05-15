-- Regression for a silent wrong-result bug in `hilbertEncode` and `mortonEncode`:
-- when the first argument was a non-constant `Tuple` column, the implementations
-- read the range mask from row 0 and used it for every row, producing wrong results.
-- Detected by `function_prop_fuzzer` as a determinism violation:
-- single-row vs multi-row execution of the same input row produced different values.
--
-- After the fix, a non-constant `Tuple` first argument is rejected explicitly,
-- mirroring the behaviour of `hilbertDecode` and `mortonDecode`. Simple-mode calls
-- (no `Tuple`) and constant-mask calls continue to work.

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

SELECT '----- hilbertEncode: multi-row non-constant Tuple mask is rejected -----';
-- Before the fix the implementation silently used row 0's mask values to drive
-- the bit-shift for every row, producing wrong results. The fuzzer flagged this
-- as a determinism violation (single-row vs multi-row gave different results
-- for the same logical input).
SELECT hilbertEncode(a, b) FROM (
    SELECT tuple(toUInt64(number)) AS a, toUInt16(3) AS b FROM numbers(3)
); -- { serverError ILLEGAL_COLUMN }
-- Two-dimensional mask with varying values per row:
SELECT hilbertEncode(a, b, c) FROM (
    SELECT (toUInt64(number), toUInt64(number + 1)) AS a, toUInt32(1024) AS b, toUInt32(16) AS c FROM numbers(4)
); -- { serverError ILLEGAL_COLUMN }

SELECT '----- mortonEncode: multi-row non-constant Tuple mask is rejected -----';
SELECT mortonEncode(a, b) FROM (
    SELECT tuple(toUInt64(number + 1)) AS a, toUInt16(3) AS b FROM numbers(3)
); -- { serverError ILLEGAL_COLUMN }
SELECT mortonEncode(a, b, c) FROM (
    SELECT (toUInt64(number + 1), toUInt64(number + 1)) AS a, toUInt32(1024) AS b, toUInt32(16) AS c FROM numbers(4)
); -- { serverError ILLEGAL_COLUMN }
