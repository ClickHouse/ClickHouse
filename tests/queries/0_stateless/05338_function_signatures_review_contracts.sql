-- The declarative signatures must not narrow or widen the contracts the executors implement.

-- `position` / `locate` read the start position with `getUInt`, so every unsigned width is accepted.
SELECT position('abcabc', 'b', toUInt128(3)), locate('b', 'abcabc', toUInt256(3)), position('abcabc', 'b', materialize(toUInt128(3)));
SELECT position('abcabc', 'b', -1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- `bitmapBuild` supports only the 8/16/32/64-bit integers.
SELECT bitmapToArray(bitmapBuild([toInt64(1), 2])), bitmapToArray(bitmapBuild([toUInt8(3)]));
SELECT toTypeName(bitmapBuild(materialize([toUInt128(1)]))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toTypeName(bitmapBuild(materialize([toInt256(1)]))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- `mapContainsKeyLike` requires `String` / `FixedString` keys.
SELECT mapContainsKeyLike(map('abc', 1), 'a%'), mapContainsKeyLike(map(toFixedString('xy', 2), 1), 'a%');
SELECT mapContainsKeyLike(map(1, 2), 'x'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
