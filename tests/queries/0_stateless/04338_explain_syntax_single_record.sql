-- Tests that EXPLAIN SYNTAX returns the reformatted query as a single multi-line record (issue #80410).

-- A multi-line reformatted query is returned as exactly one record ...
SELECT count() FROM (EXPLAIN SYNTAX SELECT 1 FROM system.one WHERE 1 IN (0, 1, 2));

-- ... and that single record holds the multi-line text, so it contains embedded newlines.
SELECT countSubstrings(explain, '\n') > 0 FROM (EXPLAIN SYNTAX SELECT 1 FROM system.one WHERE 1 IN (0, 1, 2));

-- A single-line query is still one record, without any newline.
SELECT count() FROM (EXPLAIN SYNTAX SELECT 1);

-- oneline = 1: still one record, and the query is squashed onto one physical line (no newline).
SELECT count(), countSubstrings(explain, '\n') FROM (EXPLAIN SYNTAX oneline = 1 SELECT 1 FROM system.one WHERE 1 IN (0, 1, 2)) GROUP BY explain;

-- Top-level output: one JSON row whose value holds the whole query with line feeds escaped as \n.
-- `dummy = 0` is kept by both analyzers, so the text is stable under randomized enable_analyzer.
EXPLAIN SYNTAX SELECT 1 FROM system.one WHERE dummy = 0 FORMAT JSONEachRow;
