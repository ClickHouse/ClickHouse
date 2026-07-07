-- Test lazy FINAL direct read from a text index on a ReplacingMergeTree with an is_deleted column.
-- The non-intersecting plan must both copy the index read tasks and drop is_deleted=1 rows.

DROP TABLE IF EXISTS tab;
SET query_plan_optimize_lazy_final = 1;
SET min_filtered_ratio_for_lazy_final = 0;
SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    version UInt64,
    is_deleted UInt8,
    str String,
    INDEX idx(str) TYPE text(tokenizer = array)
)
ENGINE = ReplacingMergeTree(version, is_deleted) ORDER BY id;

INSERT INTO tab VALUES (1, 1, 0, 'foo'), (2, 1, 0, 'bar'), (3, 1, 0, 'baz');
INSERT INTO tab VALUES (2, 2, 1, 'bar');  -- delete id 2
OPTIMIZE TABLE tab FINAL;

SELECT count() FROM tab FINAL WHERE str = 'bar';     -- 0: row was deleted
SELECT count() FROM tab FINAL WHERE str = 'baz';     -- 1: survivor
SELECT count() FROM tab FINAL PREWHERE str = 'baz';  -- 1: same via PREWHERE
SELECT id FROM tab FINAL WHERE str = 'foo' ORDER BY id; -- 1: survivor

DROP TABLE tab;
