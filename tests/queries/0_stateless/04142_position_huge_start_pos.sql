-- Regression test for `position` and friends with a `start_pos` close to UINT64_MAX.
-- Previously the ASCII `advancePos` helper computed `pos + n` directly, which overflowed
-- the pointer when `n` approached 2^64 and either looped indefinitely or segfaulted.

-- Vector haystack, constant needle, constant huge start_pos.
SELECT position(a, '0', CAST(18446744073709551612 AS UInt64))
FROM system.one
ARRAY JOIN ['hello0world', '0first', 'no zero here', ''] AS a;

-- Case-insensitive ASCII variant.
SELECT positionCaseInsensitive(a, 'A', CAST(18446744073709551612 AS UInt64))
FROM system.one
ARRAY JOIN ['Apple', 'banana', ''] AS a;

-- UTF-8 variants already terminated correctly, but check them too.
SELECT positionUTF8(a, 'ö', CAST(18446744073709551612 AS UInt64))
FROM system.one
ARRAY JOIN ['schön', 'plain'] AS a;

SELECT positionCaseInsensitiveUTF8(a, 'Ö', CAST(18446744073709551612 AS UInt64))
FROM system.one
ARRAY JOIN ['Schön', 'plain'] AS a;

-- Constant haystack, constant needle, vector start_pos with huge values mixed with small ones.
SELECT position('hello0world', '0', s)
FROM (SELECT arrayJoin([1, 5, 7, 18446744073709551612]) AS s);

-- Vector haystack, vector needle, constant huge start_pos.
SELECT position(h, n, CAST(18446744073709551612 AS UInt64))
FROM (SELECT arrayJoin(['hello0world', 'foobar']) AS h, arrayJoin(['0', 'b']) AS n)
ORDER BY h, n;

-- Reproducer close to the original fuzzer query.
SELECT position(a, CAST('0' AS String), CAST(18446744073709551612 AS UInt64))
FROM system.one
ARRAY JOIN CAST(['0.0.0.248','a1203d1e-a180-bd63-ffff-ffffffffffff','','plain text 0 inside','-6.11159'] AS Array(String)) AS a;
