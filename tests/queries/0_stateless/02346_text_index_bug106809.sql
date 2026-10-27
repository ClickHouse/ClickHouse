-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/106809
-- Lazy FINAL combined with a direct read from a text index dropped every matching
-- row, because its synthetic reading steps did not produce the `__text_index_*`
-- virtual column the rewritten filter relies on. With the optimization enabled the
-- counts below must stay correct.

SET query_plan_optimize_lazy_final = 1;        -- off by default; bug needs it on
SET min_filtered_ratio_for_lazy_final = 0;     -- avoid fallback to regular FINAL
SET query_plan_direct_read_from_text_index = 1; -- exercise the direct read (randomized in CI)

DROP TABLE IF EXISTS tab;

SELECT 'Single non-intersecting part: lazy FINAL must still read directly from the text index';
CREATE TABLE tab
(
    id UInt64,
    version UInt64,
    str String,
    INDEX idx(str) TYPE text(tokenizer = array)
)
ENGINE = ReplacingMergeTree(version) ORDER BY id;

INSERT INTO tab VALUES (1, 1, 'foo'), (2, 1, 'bar'), (3, 1, 'baz');
INSERT INTO tab VALUES (1, 2, 'foo_updated');
OPTIMIZE TABLE tab FINAL;

SELECT count() FROM tab FINAL WHERE str = 'bar';          -- 1: survivor, count() reads no column
SELECT count(str) FROM tab FINAL WHERE str = 'bar';       -- 1: same, but reads the column
SELECT count() FROM tab FINAL WHERE hasToken(str, 'bar'); -- 1: same rewrite via hasToken
SELECT count() FROM tab FINAL PREWHERE str = 'bar';       -- 1: direct read set up from PREWHERE
SELECT count() FROM tab FINAL WHERE str = 'foo_updated';  -- 1: updated value survives
SELECT count() FROM tab FINAL WHERE str = 'foo';          -- 0: replaced value is gone

DROP TABLE tab;

SELECT 'Mixed intersecting and non-intersecting parts: lazy FINAL falls back to a regular FINAL read';
CREATE TABLE tab
(
    id UInt64,
    version UInt64,
    str String,
    INDEX idx(str) TYPE text(tokenizer = array)
)
ENGINE = ReplacingMergeTree(version) ORDER BY id;

SYSTEM STOP MERGES tab;
INSERT INTO tab VALUES (1, 1, 'aaa'), (2, 1, 'bbb');         -- ids 1, 2
INSERT INTO tab VALUES (2, 2, 'bbb_updated'), (3, 1, 'ccc'); -- overlaps on id 2 (intersecting)
INSERT INTO tab VALUES (10, 1, 'zzz');                       -- id 10 (non-intersecting)

SELECT count() FROM tab FINAL WHERE str = 'ccc';         -- 1: survivor
SELECT count() FROM tab FINAL WHERE str = 'bbb_updated'; -- 1: updated value survives
SELECT count() FROM tab FINAL WHERE str = 'bbb';         -- 0: replaced value is gone
SELECT count() FROM tab FINAL WHERE str = 'zzz';         -- 1: from the non-intersecting part
SELECT count() FROM tab FINAL PREWHERE str = 'zzz';      -- 1: same via PREWHERE

DROP TABLE tab;
