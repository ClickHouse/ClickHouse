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
