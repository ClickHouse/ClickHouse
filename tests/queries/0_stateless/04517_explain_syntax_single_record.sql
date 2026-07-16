-- Tests the EXPLAIN SYNTAX single_record option (issue #80410).
-- single_record defaults to 1: EXPLAIN SYNTAX returns one multi-line record.
-- single_record = 0 restores the historical one-record-per-line output.

-- Default (single_record = 1): the whole reformatted query is a single record ...
SELECT count() FROM (EXPLAIN SYNTAX SELECT 1 FROM system.one WHERE 1 IN (0, 1, 2));

-- ... and that single record holds the multi-line text, so it contains embedded newlines.
SELECT countSubstrings(explain, '\n') > 0 FROM (EXPLAIN SYNTAX SELECT 1 FROM system.one WHERE 1 IN (0, 1, 2));

-- single_record = 0: the multi-line reformatted query is returned as several records (one per line).
SELECT count() > 1 FROM (EXPLAIN SYNTAX single_record = 0 SELECT 1 FROM system.one WHERE 1 IN (0, 1, 2));

-- oneline = 1: a single record without any newline.
SELECT count(), countSubstrings(explain, '\n') FROM (EXPLAIN SYNTAX oneline = 1 SELECT 1 FROM system.one WHERE 1 IN (0, 1, 2)) GROUP BY explain;

-- single_record always collapses the per-line records into exactly one, whatever the line count.
SELECT count() FROM (EXPLAIN SYNTAX SELECT 1);

-- compatibility = '26.6' predates explain_syntax_single_record, so it restores the pre-26.7
-- default (single_record = 0): the multi-line reformatted query is returned as several records
-- again. This is the upgrade-safe legacy path old clients rely on instead of editing every query.
SET compatibility = '26.6';
SELECT count() > 1 FROM (EXPLAIN SYNTAX SELECT 1 FROM system.one WHERE 1 IN (0, 1, 2));

-- The EXPLAIN-local single_record option still wins over compatibility when explicitly set.
SELECT count() FROM (EXPLAIN SYNTAX single_record = 1 SELECT 1 FROM system.one WHERE 1 IN (0, 1, 2));
