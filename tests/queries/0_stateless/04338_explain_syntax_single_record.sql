-- Tests the EXPLAIN SYNTAX single_record option (issue #80410).
-- Default: one record per line (historical behavior). single_record = 1: one multi-line record.

-- Default: a multi-line reformatted query is returned as several records (one per line).
SELECT count() > 1 FROM (EXPLAIN SYNTAX SELECT 1 FROM system.one WHERE 1 IN (0, 1, 2));

-- single_record = 1: the whole reformatted query is a single record ...
SELECT count() FROM (EXPLAIN SYNTAX single_record = 1 SELECT 1 FROM system.one WHERE 1 IN (0, 1, 2));

-- ... and that single record holds the multi-line text, so it contains embedded newlines.
SELECT countSubstrings(explain, '\n') > 0 FROM (EXPLAIN SYNTAX single_record = 1 SELECT 1 FROM system.one WHERE 1 IN (0, 1, 2));

-- single_record = 1 with oneline = 1: a single record without any newline.
SELECT count(), countSubstrings(explain, '\n') FROM (EXPLAIN SYNTAX single_record = 1, oneline = 1 SELECT 1 FROM system.one WHERE 1 IN (0, 1, 2)) GROUP BY explain;

-- single_record always collapses the per-line records into exactly one, whatever the line count.
SELECT count() FROM (EXPLAIN SYNTAX single_record = 1 SELECT 1);

-- Top-level output: single_record = 1 yields one JSON row with the line feeds escaped as \n.
-- `dummy = 0` is kept by both analyzers, so the text is stable under randomized enable_analyzer.
EXPLAIN SYNTAX single_record = 1 SELECT 1 FROM system.one WHERE dummy = 0 FORMAT JSONEachRow;

-- Default: the same query is returned as several JSON rows, one per line.
EXPLAIN SYNTAX SELECT 1 FROM system.one WHERE dummy = 0 FORMAT JSONEachRow;
