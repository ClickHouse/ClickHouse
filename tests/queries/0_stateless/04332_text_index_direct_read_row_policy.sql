-- Regression test for a prewhere column mismatch in the text index direct read
-- optimization (`query_plan_direct_read_from_text_index`) when combined with a
-- row-level filter (a `ROW POLICY`) that reads a String column.
--
-- The text index read is a `None`-type prewhere step. Its actions were executed on
-- the sample block in the `MergeTreeRangeReader` constructor, but skipped at runtime
-- by `executePrewhereActionsAndFilterColumns`. With a preceding row-level filter that
-- reads a String column, the sample-block column order diverged from the actual data,
-- so the raw String column landed in a filter position and the query failed with
-- `ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER`.

SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    category LowCardinality(String),
    message String,
    INDEX idx_message message TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (category, id);

INSERT INTO tab SELECT number, 'a', concat('hello world token ', toString(number)) FROM numbers(1000);

DROP ROW POLICY IF EXISTS pol_04332 ON tab;
CREATE ROW POLICY pol_04332 ON tab FOR SELECT USING category != 'hidden' TO ALL;

-- The row policy reads the String column `category` before the text index `None` step.
-- This used to raise `ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER`; it must now return 1000.
SELECT count() FROM tab WHERE category = 'a' AND hasToken(message, 'token');

-- Sanity check: the same query without the direct read optimization returns the same result.
SELECT count() FROM tab WHERE category = 'a' AND hasToken(message, 'token')
SETTINGS query_plan_direct_read_from_text_index = 0;

DROP ROW POLICY pol_04332 ON tab;
DROP TABLE tab;
