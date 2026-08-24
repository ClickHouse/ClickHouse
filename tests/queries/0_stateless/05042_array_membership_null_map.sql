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

SELECT '-- two arrays that print identically must still answer differently --';
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

SELECT '-- an array long enough to use the vectorised loop, not only the remainder --';
WITH arrayMap(x -> nullIf(x, toInt32(42)), arrayMap(i -> if(i = 20, toInt32(42), toInt32(i)), range(40))) AS h
SELECT length(h), has(h, 42) AS has, hasAll(h, [42]), hasAny(h, [42]), hasAll(h, [NULL]);

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
