-- Test lazy FINAL direct read from a text index on a ReplacingMergeTree with an is_deleted column.
-- The non-intersecting plan must both copy the index read tasks and drop is_deleted=1 rows.

DROP TABLE IF EXISTS tab;
-- Lazy FINAL is gated on the new analyzer (QueryPlanOptimizationSettings:
-- optimize_lazy_final = query_plan_optimize_lazy_final && allow_experimental_analyzer),
-- so in the old-analyzer CI lane the optimization never fires and the plan-shape
-- assertions below (union / is_deleted filter) read 0 while results stay correct. Pin it on.
SET enable_analyzer = 1;
SET query_plan_optimize_lazy_final = 1;
SET min_filtered_ratio_for_lazy_final = 0;
SET query_plan_direct_read_from_text_index = 1;
-- clickhouse-test randomizes use_skip_indexes_if_final; at 0 skip indexes are
-- disabled under FINAL, so the direct text-index read is never produced and the
-- interaction under test is not covered. Pin it on.
SET use_skip_indexes_if_final = 1;
-- Same reasoning for the two settings below (also randomized by clickhouse-test):
-- at exact_mode=0 the text index prunes the middle part outright, so parts 1+3 no
-- longer intersect and the plan collapses to the fully-replaced branch (no union);
-- at optimize_on_insert=0 parts aren't pre-replaced at insert so the
-- non-intersecting split never happens. Both break the union assertions below.
SET use_skip_indexes_if_final_exact_mode = 1;
SET optimize_on_insert = 1;

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

-- Mixed intersecting and non-intersecting parts with is_deleted. The single-token
-- queries below do NOT produce a union: text-index pruning drops the other side's
-- parts before the plan split, so each collapses to one side -- 'aaa'/'bbb'/'ccc'
-- to the intersecting regular-FINAL replacing read, 'zzz' to the non-intersecting
-- direct-read + is_deleted branch (verified via EXPLAIN). They still assert correct
-- replacement/deletion on both sides. The union itself is exercised separately below.
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

-- A query spanning both sides ('aaa' intersecting + 'zzz' non-intersecting) is what
-- actually yields the union of the regular-FINAL and non-intersecting plans. The IN
-- filter stays above the union (not rewritten to a direct index read), so this covers
-- the union path together with the deletion filter (`is_deleted = 0`), not a direct read.
SELECT count() FROM tab FINAL WHERE str IN ('aaa', 'zzz');  -- 2: one survivor per side

SELECT 'union in plan', count() > 0 FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab FINAL WHERE str IN ('aaa', 'zzz')
)
WHERE explain LIKE '%Union%';

SELECT 'is_deleted filter in union plan', count() > 0 FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab FINAL WHERE str IN ('aaa', 'zzz')
)
WHERE explain LIKE '%is_deleted = 0%';

DROP TABLE tab;
