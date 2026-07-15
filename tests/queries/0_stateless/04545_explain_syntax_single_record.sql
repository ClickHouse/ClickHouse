-- Tests that EXPLAIN SYNTAX returns the pretty-printed (multi-line) query as a single
-- record by default, and that the `oneline` option collapses it to one physical line (issue #80410).

-- Default: one record whose value is the multi-line reformatted query.
SELECT count() AS records, countMatches(any(explain), '\n') > 0 AS is_multiline
FROM (EXPLAIN SYNTAX SELECT number FROM numbers(10) WHERE number > 1 GROUP BY number ORDER BY number);

-- oneline = 1: still a single record, collapsed to one physical line.
SELECT count() AS records, countMatches(any(explain), '\n') AS newlines
FROM (EXPLAIN SYNTAX oneline = 1 SELECT number FROM numbers(10) WHERE number > 1 GROUP BY number ORDER BY number);

-- The full multi-line output, verifying newlines are embedded in the single cell.
EXPLAIN SYNTAX SELECT number FROM numbers(10) WHERE number > 1 GROUP BY number ORDER BY number;
