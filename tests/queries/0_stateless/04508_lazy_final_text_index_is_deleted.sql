-- Test lazy FINAL direct read from a text index on a ReplacingMergeTree with an is_deleted column.
-- The non-intersecting plan must both copy the index read tasks and drop is_deleted=1 rows.

DROP TABLE IF EXISTS tab;
SET query_plan_optimize_lazy_final = 1;
SET min_filtered_ratio_for_lazy_final = 0;
SET query_plan_direct_read_from_text_index = 1;
-- clickhouse-test randomizes use_skip_indexes_if_final; at 0 skip indexes are
-- disabled under FINAL, so the direct text-index read is never produced and the
-- interaction under test is not covered. Pin it on.
SET use_skip_indexes_if_final = 1;

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

-- Keep this test from silently going dead if lazy FINAL stops applying here:
-- assert the non-intersecting plan carries BOTH the direct text-index read
-- (`__text_index_idx_*` virtual column, MergeTreeIndexConditionText.cpp) AND the
-- deletion filter (`is_deleted = 0`, added by optimizeLazyFinal.cpp).
-- If either disappears, the counts above could stay green for the wrong reason.
SELECT 'text-index read in plan', count() > 0 FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab FINAL WHERE str = 'baz'
)
WHERE explain LIKE '%__text_index_idx_%';

SELECT 'is_deleted filter in plan', count() > 0 FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab FINAL WHERE str = 'baz'
)
WHERE explain LIKE '%is_deleted = 0%';

DROP TABLE tab;

-- Mixed intersecting and non-intersecting parts with is_deleted: the intersecting
-- pair (parts 1+2 overlap on id 2) forces the regular-FINAL branch, while the
-- non-intersecting part 3 (id 10) takes the direct-read + is_deleted branch. Lazy
-- FINAL unions the two, so this exercises the union path with a deletion filter.
CREATE TABLE tab
(
    id UInt64,
    version UInt64,
    is_deleted UInt8,
    str String,
    INDEX idx(str) TYPE text(tokenizer = array)
)
ENGINE = ReplacingMergeTree(version, is_deleted) ORDER BY id;

SYSTEM STOP MERGES tab;
INSERT INTO tab VALUES (1, 1, 0, 'aaa'), (2, 1, 0, 'bbb');  -- ids 1, 2
INSERT INTO tab VALUES (2, 2, 1, 'bbb'), (3, 1, 0, 'ccc');  -- overlaps id 2 and deletes it (intersecting)
INSERT INTO tab VALUES (10, 1, 0, 'zzz');                   -- id 10 (non-intersecting)

SELECT count() FROM tab FINAL WHERE str = 'aaa';    -- 1: survivor from the intersecting pair
SELECT count() FROM tab FINAL WHERE str = 'bbb';    -- 0: id 2 deleted by the newer version
SELECT count() FROM tab FINAL WHERE str = 'ccc';    -- 1: survivor from the intersecting pair
SELECT count() FROM tab FINAL WHERE str = 'zzz';    -- 1: from the non-intersecting part
SELECT count() FROM tab FINAL PREWHERE str = 'zzz'; -- 1: same via PREWHERE

DROP TABLE tab;
