-- Tags: no-fasttest
-- no-fasttest: the RawBLOB format is not available in fast test builds.

-- `formatRowNoNewline` strips a trailing newline from each row. It must never rewind past the start of the
-- current row into the previous row's bytes. Otherwise a row that emits no bytes (e.g. an empty string with the
-- `RawBLOB` format) produces non-monotonic `ColumnString` offsets and a `size_t` underflow in the string size.

-- An empty row right after a non-empty one that ended with a newline must stay empty (length 0), not underflow.
SELECT length(formatRowNoNewline('RawBLOB', s)) AS len
FROM (SELECT arrayJoin(['a\n\n', '']) AS s)
ORDER BY ALL;

-- The bytes of a following row must be exactly those the row emitted (no cross-row bleed).
SELECT hex(formatRowNoNewline('RawBLOB', s)) AS bytes
FROM (SELECT arrayJoin(['a\n\n', '', 'b']) AS s)
ORDER BY ALL;

-- Several consecutive empty rows around non-empty ones stay empty and keep offsets monotonic.
SELECT length(formatRowNoNewline('RawBLOB', s)) AS len
FROM (SELECT arrayJoin(['x\n', '', '', 'y\n', '']) AS s)
ORDER BY ALL;

-- Regression guard for the newline-stripping itself: it must keep working for rows after the internal write
-- buffer has grown past its initial size and been flushed at least once (which happens from the second row on
-- for these sizes). Every row must have its trailing newline stripped, so all lengths are equal.
SELECT DISTINCT length(formatRowNoNewline('TSV', repeat('z', 50))) AS len FROM numbers(5);
SELECT DISTINCT length(formatRowNoNewline('CSV', number, repeat('w', 40))) AS len FROM numbers(5);

-- Normal row formats keep their usual behavior.
SELECT formatRowNoNewline('TSV', 1, 2, 'good') AS f;
SELECT formatRowNoNewline('CSV', number, 'good') FROM numbers(3);
