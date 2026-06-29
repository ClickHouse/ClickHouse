-- Regression test for `arrayReverseFill` crash on empty sub-arrays with string elements.
-- See STID 0988-292b (rare chronic trunk bug, same family as issue #12263 from 2020).
--
-- With a constant-false lambda on `Array(String)` that contains empty rows,
-- the `column_fill_const` branch of `ArrayFillImpl::execute` computed
-- `array_end = in_offset - 1`, which underflowed to `SIZE_MAX` when the first row
-- was empty (`in_offset == 0`). The resulting `insertManyFrom(src, SIZE_MAX, 0)`
-- call made `ColumnString::doInsertManyFrom` eagerly read
-- `offsets[SIZE_MAX - 1]`, violating the `PODArray` bounds assertion and aborting
-- the server. `arrayFill` avoided the crash accidentally because it passes
-- `array_begin` (`= 0`) as `position`, but the logic was still incorrect.

DROP TABLE IF EXISTS t_array_fill_empty_strings;

CREATE TABLE t_array_fill_empty_strings (id UInt32, a Array(String)) ENGINE = Memory;
INSERT INTO t_array_fill_empty_strings VALUES (1, []), (2, ['a']), (3, ['b', 'c']), (4, []), (5, ['d']);

-- Constant-false lambda exercises the buggy `column_fill_const` branch.
SELECT id, arrayReverseFill(x -> toUInt8(0), a) FROM t_array_fill_empty_strings ORDER BY id;
SELECT id, arrayFill(x -> toUInt8(0), a) FROM t_array_fill_empty_strings ORDER BY id;

-- Constant-true lambda returns the input unchanged (fast path).
SELECT id, arrayReverseFill(x -> toUInt8(1), a) FROM t_array_fill_empty_strings ORDER BY id;
SELECT id, arrayFill(x -> toUInt8(1), a) FROM t_array_fill_empty_strings ORDER BY id;

-- Non-constant lambda exercises the `column_fill` branch for completeness.
SELECT id, arrayReverseFill(x -> x != 'a', a) FROM t_array_fill_empty_strings ORDER BY id;
SELECT id, arrayFill(x -> x != 'a', a) FROM t_array_fill_empty_strings ORDER BY id;

DROP TABLE t_array_fill_empty_strings;

-- Single-row empty-array variants (direct regression for #12263 with `String` element type).
SELECT arrayReverseFill(x -> toUInt8(0), materialize(CAST([] AS Array(String))));
SELECT arrayFill(x -> toUInt8(0), materialize(CAST([] AS Array(String))));
