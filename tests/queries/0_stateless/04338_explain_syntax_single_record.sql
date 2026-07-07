-- Tests that EXPLAIN SYNTAX returns the reformatted query as a single record (issue #80410).

-- The reformatted query is returned as exactly one record ...
SELECT count() FROM (EXPLAIN SYNTAX SELECT 1 FROM system.one WHERE 1 IN (0, 1, 2));

-- ... and by default (oneline = 1) that record is squashed onto one physical line, no newline.
SELECT count(), countSubstrings(explain, '\n') FROM (EXPLAIN SYNTAX SELECT 1 FROM system.one WHERE 1 IN (0, 1, 2)) GROUP BY explain;

-- A single-line query is still one record, without any newline.
SELECT count() FROM (EXPLAIN SYNTAX SELECT 1);

-- oneline = 0: still exactly one record, but the text is spread across multiple lines,
-- so the single record contains embedded newlines.
SELECT count(), countSubstrings(explain, '\n') > 0 FROM (EXPLAIN SYNTAX oneline = 0 SELECT 1 FROM system.one WHERE 1 IN (0, 1, 2)) GROUP BY explain;

-- Default (oneline) top-level output: one clean row on a single physical line in every format,
-- with no embedded line feeds to escape. `dummy = 0` is kept by both analyzers, so the text is
-- stable under randomized enable_analyzer.
EXPLAIN SYNTAX SELECT 1 FROM system.one WHERE dummy = 0 FORMAT JSONEachRow;
EXPLAIN SYNTAX SELECT 1 FROM system.one WHERE dummy = 0 FORMAT TSV;
EXPLAIN SYNTAX SELECT 1 FROM system.one WHERE dummy = 0 FORMAT TSVRaw;

-- oneline = 0 top-level output: one JSON row whose value holds the whole multi-line query with
-- line feeds escaped as \n.
EXPLAIN SYNTAX oneline = 0 SELECT 1 FROM system.one WHERE dummy = 0 FORMAT JSONEachRow;

-- oneline = 0 with raw line formats (TSVRaw/Raw/LineAsString) does not escape the embedded line
-- feeds, so the multi-line record prints as several physical lines. This is inherent to raw
-- formats and is byte-for-byte identical to the pre-#80410 per-line output, so it is not a
-- regression: any multi-line String is non-round-trippable through a line-delimited raw format.
EXPLAIN SYNTAX oneline = 0 SELECT 1 FROM system.one WHERE dummy = 0 FORMAT TSVRaw;

-- oneline = 0 display formats (Vertical, and the interactive Pretty* default) render the single
-- record across physical lines with literal newlines, so the reformatted query is visibly
-- multi-line. Only the line-delimited escaping text formats (TSV/CSV) show the \n as the
-- escaped sequence.
EXPLAIN SYNTAX oneline = 0 SELECT 1 FROM system.one WHERE dummy = 0 FORMAT Vertical;
