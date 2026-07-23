-- Validates the one-byte-needle/one-byte-replacement "replace all" fast path in ReplaceStringImpl
-- (reached via replaceRegexpAll/replaceAll with a trivial single-char pattern). The fast path must
-- produce identical results to the general path and to an independent oracle.

-- replaceRegexpAll with a trivial one-char pattern goes through the fallback fast path.
SELECT replaceRegexpAll(materialize('a b c d'), ' ', '_');
SELECT replaceRegexpAll(materialize('no_spaces_here'), ' ', '_');
SELECT replaceRegexpAll(materialize('     '), ' ', '_');
SELECT replaceRegexpAll(materialize(''), ' ', '_');
SELECT replaceRegexpAll(materialize('a  b'), ' ', 'X');  -- adjacent matches

-- replaceAll (literal) takes the same fast path.
SELECT replaceAll(materialize('a.b.c'), '.', ',');

-- Cross-check the fast path against an independent oracle over many rows.
SELECT count()
FROM
(
    WITH 'Many years later as he faced the firing squad, Colonel Aureliano Buendia.' AS s
    SELECT replaceRegexpAll(materialize(s), ' ', '\n') AS w,
           arrayStringConcat(splitByChar(' ', s), '\n') AS oracle
    FROM numbers(1000)
)
WHERE w != oracle;

-- Fast path must NOT alter rows without the needle, and must keep row boundaries intact.
SELECT count()
FROM
(
    SELECT replaceRegexpAll(materialize(toString(number)), ' ', '_') AS w, toString(number) AS oracle
    FROM numbers(1000)
)
WHERE w != oracle;

-- replaceRegexpOne (First mode) must NOT use the all-mode fast path: only the first match changes.
SELECT replaceRegexpOne(materialize('a b c'), ' ', '_');

-- One-byte needle but multi-byte replacement must go through the general path, not the fast path.
SELECT replaceRegexpAll(materialize('a b'), ' ', 'XY');

-- Trivial multi-byte needle still uses the fallback but the general loop, not the one-byte fast path.
SELECT replaceRegexpAll(materialize('a, b, c'), ', ', '|');

-- FixedString haystack still works (separate code path, unchanged).
SELECT replaceRegexpAll(materialize(toFixedString('a b c', 5)), ' ', '_');

-- NUL byte cases. Row boundaries in ColumnString are defined by offsets, not by an in-band
-- terminator, so the fast path is safe for a NUL needle and for NUL bytes inside the payload.
-- Oracles are built with concat (no string-replace machinery) for an independent cross-check.

-- NUL needle: replace embedded NUL bytes.
SELECT replaceAll(materialize(concat('a', char(0), 'b', char(0), 'c')), char(0), '_') = 'a_b_c';
-- Leading and trailing NUL needle (the byte that used to be the row terminator).
SELECT replaceAll(materialize(concat(char(0), 'x', char(0))), char(0), '.') = '.x.';
-- NUL inside the payload must be preserved when replacing a different byte.
SELECT replaceAll(materialize(concat('a', char(0), ' b')), ' ', '_') = concat('a', char(0), '_b');

-- Many-row cross-check: NUL needle, oracle via concat.
SELECT count()
FROM
(
    SELECT replaceAll(materialize(concat(toString(number), char(0), toString(number + 1))), char(0), '|') AS w,
           concat(toString(number), '|', toString(number + 1)) AS oracle
    FROM numbers(1000)
)
WHERE w != oracle;

-- Many-row cross-check: NUL in payload preserved while replacing a different byte.
SELECT count()
FROM
(
    SELECT replaceAll(materialize(concat(toString(number), char(0), 'x y')), ' ', '_') AS w,
           concat(toString(number), char(0), 'x_y') AS oracle
    FROM numbers(1000)
)
WHERE w != oracle;

-- needle == replacement is a no-op for any needle length and both modes: copy the column verbatim
-- and skip the search. Output must be byte-identical to the input.

-- One-byte needle == replacement (the case raised in review).
SELECT replaceAll(materialize('a b c'), ' ', ' ');
-- Multi-byte needle == replacement also short-circuits.
SELECT replaceAll(materialize('a, b, c'), ', ', ', ');
-- First mode (replaceOne) with needle == replacement is likewise a no-op.
SELECT replaceOne(materialize('a b c'), ' ', ' ');
-- FixedString haystack with needle == replacement: output unchanged.
SELECT replaceAll(materialize(toFixedString('a b c', 5)), ' ', ' ');
-- NUL needle == NUL replacement: payload preserved exactly.
SELECT replaceAll(materialize(concat('a', char(0), 'b')), char(0), char(0)) = concat('a', char(0), 'b');
-- Many-row cross-check: needle == replacement leaves every row identical to the input.
SELECT count()
FROM
(
    SELECT replaceAll(materialize(toString(number)), '0', '0') AS w, toString(number) AS oracle
    FROM numbers(1000)
)
WHERE w != oracle;
