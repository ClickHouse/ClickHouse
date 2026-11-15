-- A NULL array element must be treated as NULL by hasAll, hasAny, hasSubstr, startsWith and
-- endsWith, never as the value physically stored beneath it. Every arm prints the answer of
-- has() next to it, because has() is already correct and makes the expected answer self-evident.

SELECT '-- false positive: needle equals the value hidden under the NULL --';
-- h is [NULL] with 42 stored underneath. Nothing here contains 42.
WITH arrayMap(x -> nullIf(x, toInt32(42)), [toInt32(42)]) AS h
SELECT has(h, 42) AS has, hasAll(h, [42]), hasAny(h, [42]), hasSubstr(h, [42]),
       startsWith(h, [42]), endsWith(h, [42]);

SELECT '-- false negative: NULL needle over a non-zero hidden value --';
WITH arrayMap(x -> nullIf(x, toInt32(42)), [toInt32(42)]) AS h
SELECT has(h, NULL) AS has, hasAll(h, [NULL]), hasAny(h, [NULL]), hasSubstr(h, [NULL]),
       startsWith(h, [NULL]), endsWith(h, [NULL]);

SELECT '-- two arrays a user cannot tell apart must answer identically --';
WITH arrayMap(x -> nullIf(x, toInt32(42)), [toInt32(42), 5]) AS a,
     arrayMap(x -> nullIf(x, toInt32(99)), [toInt32(99), 5]) AS b
SELECT toString(a) = toString(b) AS printed_alike, hasAll(a, [42]), hasAll(b, [42]),
       hasAll(a, [99]), hasAll(b, [99]);

SELECT '-- all eight integer widths --';
SELECT hasAll(arrayMap(x -> nullIf(x, toInt8(42)), [toInt8(42)]), [toInt8(42)]),
       hasAll(arrayMap(x -> nullIf(x, toInt16(42)), [toInt16(42)]), [toInt16(42)]),
       hasAll(arrayMap(x -> nullIf(x, toInt32(42)), [toInt32(42)]), [toInt32(42)]),
       hasAll(arrayMap(x -> nullIf(x, toInt64(42)), [toInt64(42)]), [toInt64(42)]),
       hasAll(arrayMap(x -> nullIf(x, toUInt8(42)), [toUInt8(42)]), [toUInt8(42)]),
       hasAll(arrayMap(x -> nullIf(x, toUInt16(42)), [toUInt16(42)]), [toUInt16(42)]),
       hasAll(arrayMap(x -> nullIf(x, toUInt32(42)), [toUInt32(42)]), [toUInt32(42)]),
       hasAll(arrayMap(x -> nullIf(x, toUInt64(42)), [toUInt64(42)]), [toUInt64(42)]);

SELECT '-- a long haystack with a one-element needle, which is the remainder path --';
WITH arrayMap(x -> nullIf(x, toInt32(42)), arrayMap(i -> if(i = 20, toInt32(42), toInt32(i)), range(40))) AS h
SELECT length(h), has(h, 42) AS has, hasAll(h, [42]), hasAny(h, [42]), hasAll(h, [NULL]);

SELECT '-- needles long enough to enter the vectorised loop, on every width --';
-- hasAll admits its vector loop only when BOTH sizes exceed a per-width threshold (3 for
-- 64-bit, 7 for 32-bit, 15 for 16- and 8-bit), and it is the NEEDLE size that is compared
-- against it. A 16-element needle clears all four, and 22 also leaves a remainder on all
-- four, so the vector loop and the hand-off to the remainder loop are both covered.
-- The haystack is 1..24, every value genuinely present, plus one NULL hiding 100. So a
-- needle containing 100 can only fail because the NULL is respected, and the arms whose
-- needles hold none of it must keep matching.
-- The payload arms put that NULL at index 0, inside the first vector iteration, so they
-- exercise the haystack null map's masking rather than the scalar tail that follows it,
-- while needle_null_16 puts a NULL in the needle and exercises the needle-side mask.
WITH arrayMap(x -> nullIf(x, toInt8(100)), arrayMap(i -> toInt8(if(i = 0, 100, i)), range(25))) AS h
SELECT 'Int8' AS width, has(h, toInt8(100)) AS has,
       hasAll(h, arrayMap(i -> toInt8(if(i = 0, 100, i)), range(16))) AS payload_16,
       hasAll(h, arrayMap(i -> toInt8(if(i = 0, 100, i)), range(22))) AS payload_22,
       hasAll(h, arrayMap(i -> toInt8(i + 1), range(16))) AS present_16,
       hasAll(h, arrayMap(i -> toInt8(i + 1), range(22))) AS present_22,
       hasAll(h, arrayMap(x -> nullIf(x, toInt8(99)), arrayConcat([toInt8(99)], arrayMap(i -> toInt8(i + 1), range(15))))) AS needle_null_16;
WITH arrayMap(x -> nullIf(x, toInt16(100)), arrayMap(i -> toInt16(if(i = 0, 100, i)), range(25))) AS h
SELECT 'Int16' AS width, has(h, toInt16(100)) AS has,
       hasAll(h, arrayMap(i -> toInt16(if(i = 0, 100, i)), range(16))) AS payload_16,
       hasAll(h, arrayMap(i -> toInt16(if(i = 0, 100, i)), range(22))) AS payload_22,
       hasAll(h, arrayMap(i -> toInt16(i + 1), range(16))) AS present_16,
       hasAll(h, arrayMap(i -> toInt16(i + 1), range(22))) AS present_22,
       hasAll(h, arrayMap(x -> nullIf(x, toInt16(99)), arrayConcat([toInt16(99)], arrayMap(i -> toInt16(i + 1), range(15))))) AS needle_null_16;
WITH arrayMap(x -> nullIf(x, toInt32(100)), arrayMap(i -> toInt32(if(i = 0, 100, i)), range(25))) AS h
SELECT 'Int32' AS width, has(h, toInt32(100)) AS has,
       hasAll(h, arrayMap(i -> toInt32(if(i = 0, 100, i)), range(16))) AS payload_16,
       hasAll(h, arrayMap(i -> toInt32(if(i = 0, 100, i)), range(22))) AS payload_22,
       hasAll(h, arrayMap(i -> toInt32(i + 1), range(16))) AS present_16,
       hasAll(h, arrayMap(i -> toInt32(i + 1), range(22))) AS present_22,
       hasAll(h, arrayMap(x -> nullIf(x, toInt32(99)), arrayConcat([toInt32(99)], arrayMap(i -> toInt32(i + 1), range(15))))) AS needle_null_16;
WITH arrayMap(x -> nullIf(x, toInt64(100)), arrayMap(i -> toInt64(if(i = 0, 100, i)), range(25))) AS h
SELECT 'Int64' AS width, has(h, toInt64(100)) AS has,
       hasAll(h, arrayMap(i -> toInt64(if(i = 0, 100, i)), range(16))) AS payload_16,
       hasAll(h, arrayMap(i -> toInt64(if(i = 0, 100, i)), range(22))) AS payload_22,
       hasAll(h, arrayMap(i -> toInt64(i + 1), range(16))) AS present_16,
       hasAll(h, arrayMap(i -> toInt64(i + 1), range(22))) AS present_22,
       hasAll(h, arrayMap(x -> nullIf(x, toInt64(99)), arrayConcat([toInt64(99)], arrayMap(i -> toInt64(i + 1), range(15))))) AS needle_null_16;
WITH arrayMap(x -> nullIf(x, toUInt8(100)), arrayMap(i -> toUInt8(if(i = 0, 100, i)), range(25))) AS h
SELECT 'UInt8' AS width, has(h, toUInt8(100)) AS has,
       hasAll(h, arrayMap(i -> toUInt8(if(i = 0, 100, i)), range(16))) AS payload_16,
       hasAll(h, arrayMap(i -> toUInt8(if(i = 0, 100, i)), range(22))) AS payload_22,
       hasAll(h, arrayMap(i -> toUInt8(i + 1), range(16))) AS present_16,
       hasAll(h, arrayMap(i -> toUInt8(i + 1), range(22))) AS present_22,
       hasAll(h, arrayMap(x -> nullIf(x, toUInt8(99)), arrayConcat([toUInt8(99)], arrayMap(i -> toUInt8(i + 1), range(15))))) AS needle_null_16;
WITH arrayMap(x -> nullIf(x, toUInt16(100)), arrayMap(i -> toUInt16(if(i = 0, 100, i)), range(25))) AS h
SELECT 'UInt16' AS width, has(h, toUInt16(100)) AS has,
       hasAll(h, arrayMap(i -> toUInt16(if(i = 0, 100, i)), range(16))) AS payload_16,
       hasAll(h, arrayMap(i -> toUInt16(if(i = 0, 100, i)), range(22))) AS payload_22,
       hasAll(h, arrayMap(i -> toUInt16(i + 1), range(16))) AS present_16,
       hasAll(h, arrayMap(i -> toUInt16(i + 1), range(22))) AS present_22,
       hasAll(h, arrayMap(x -> nullIf(x, toUInt16(99)), arrayConcat([toUInt16(99)], arrayMap(i -> toUInt16(i + 1), range(15))))) AS needle_null_16;
WITH arrayMap(x -> nullIf(x, toUInt32(100)), arrayMap(i -> toUInt32(if(i = 0, 100, i)), range(25))) AS h
SELECT 'UInt32' AS width, has(h, toUInt32(100)) AS has,
       hasAll(h, arrayMap(i -> toUInt32(if(i = 0, 100, i)), range(16))) AS payload_16,
       hasAll(h, arrayMap(i -> toUInt32(if(i = 0, 100, i)), range(22))) AS payload_22,
       hasAll(h, arrayMap(i -> toUInt32(i + 1), range(16))) AS present_16,
       hasAll(h, arrayMap(i -> toUInt32(i + 1), range(22))) AS present_22,
       hasAll(h, arrayMap(x -> nullIf(x, toUInt32(99)), arrayConcat([toUInt32(99)], arrayMap(i -> toUInt32(i + 1), range(15))))) AS needle_null_16;
WITH arrayMap(x -> nullIf(x, toUInt64(100)), arrayMap(i -> toUInt64(if(i = 0, 100, i)), range(25))) AS h
SELECT 'UInt64' AS width, has(h, toUInt64(100)) AS has,
       hasAll(h, arrayMap(i -> toUInt64(if(i = 0, 100, i)), range(16))) AS payload_16,
       hasAll(h, arrayMap(i -> toUInt64(if(i = 0, 100, i)), range(22))) AS payload_22,
       hasAll(h, arrayMap(i -> toUInt64(i + 1), range(16))) AS present_16,
       hasAll(h, arrayMap(i -> toUInt64(i + 1), range(22))) AS present_22,
       hasAll(h, arrayMap(x -> nullIf(x, toUInt64(99)), arrayConcat([toUInt64(99)], arrayMap(i -> toUInt64(i + 1), range(15))))) AS needle_null_16;

SELECT '-- String and Tuple elements --';
WITH arrayMap(x -> nullIf(x, 'zz'), ['zz']) AS h
SELECT has(h, 'zz') AS has, hasAll(h, ['zz']), hasAny(h, ['zz']), hasSubstr(h, ['zz']),
       startsWith(h, ['zz']), hasAll(h, [NULL]);
WITH arrayMap(x -> nullIf(x, (toInt32(1), 'a')), [(toInt32(1), 'a')]) AS h
SELECT has(h, (1, 'a')) AS has, hasAll(h, [(1, 'a')]), hasAny(h, [(1, 'a')]), hasAll(h, [NULL]);

SELECT '-- constant, materialized haystack, and materialized needle --';
SELECT hasAll(arrayMap(x -> nullIf(x, toInt32(42)), [toInt32(42)]), [toInt32(42)]),
       hasAll(materialize(arrayMap(x -> nullIf(x, toInt32(42)), [toInt32(42)])), [toInt32(42)]),
       hasAll(arrayMap(x -> nullIf(x, toInt32(42)), [toInt32(42)]), materialize([toInt32(42)]));

SELECT '-- every startsWith and endsWith name --';
WITH arrayMap(x -> nullIf(x, toInt32(42)), [toInt32(42)]) AS h
SELECT startsWith(h, [42]), startsWithCaseInsensitive(h, [42]), startsWithUTF8(h, [42]),
       startsWithCaseInsensitiveUTF8(h, [42]), endsWith(h, [42]), endsWithCaseInsensitive(h, [42]),
       endsWithUTF8(h, [42]), endsWithCaseInsensitiveUTF8(h, [42]);

SELECT '-- on a real column rather than a literal --';
DROP TABLE IF EXISTS t_null_map;
CREATE TABLE t_null_map (id UInt32, v Array(Nullable(Int32))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_null_map SELECT 1, arrayMap(x -> nullIf(x, toInt32(42)), [toInt32(42), 5]);
INSERT INTO t_null_map SELECT 2, [toInt32(42), 5];
SELECT id, has(v, 42) AS has, hasAll(v, [42]), hasAny(v, [42]), hasAll(v, [NULL])
FROM t_null_map ORDER BY id;
DROP TABLE t_null_map;

SELECT '-- controls: no NULL present, non-nullable haystack, needle absent --';
SELECT hasAll([1, 2, 3], [2]), hasAll(materialize([1, 2, 3]), [2]), hasAll([1, 2, 3], [0]),
       hasAll(arrayMap(x -> nullIf(x, toInt32(42)), [toInt32(42)]), [777]);
SELECT '-- controls: NULL matches NULL, and a NULL needle over a haystack without NULL --';
SELECT hasAll([1, NULL], [NULL]), hasSubstr([1, NULL], [NULL]), startsWith([NULL, 2], [NULL]),
       hasAll(materialize([1, 2, 3]), [NULL]), hasAny(materialize([1, 2, 3]), [NULL]);
SELECT '-- controls: prefix and suffix still work --';
SELECT startsWith([1, 2, 3], [1, 2]), startsWith([1, 2, 3], [2, 3]),
       endsWith([1, 2, 3], [2, 3]), endsWith([1, 2, 3], [1, 2]);
