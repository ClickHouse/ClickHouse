-- `unhex` of an odd-width `FixedString` pads the incomplete leading group of every row,
-- exactly like `unhex` of a `String` with the same content.

SELECT hex(unhex(materialize(toFixedString(s, 3)))) FROM (SELECT arrayJoin(['abc', 'ABC', '123', 'fff', '0a0']) AS s);
SELECT hex(unhex(materialize(toFixedString(s, 1)))) FROM (SELECT arrayJoin(['0', '7', 'f', 'F']) AS s);

-- Many rows of various odd widths: compare with the `String` path.
SELECT 1, countIf(unhex(toFixedString(substring(hex(sipHash128(number)), 1, 1), 1)) != unhex(substring(hex(sipHash128(number)), 1, 1))) FROM numbers(10000);
SELECT 3, countIf(unhex(toFixedString(substring(hex(sipHash128(number)), 1, 3), 3)) != unhex(substring(hex(sipHash128(number)), 1, 3))) FROM numbers(10000);
SELECT 31, countIf(unhex(toFixedString(substring(hex(sipHash128(number)), 1, 31), 31)) != unhex(substring(hex(sipHash128(number)), 1, 31))) FROM numbers(10000);

-- Empty column.
SELECT unhex(toFixedString('abc', 3)) FROM numbers(0);
