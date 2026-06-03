SET allow_experimental_projection_text_index = 1;
SET enable_full_text_index = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;

-- Regression test for pattern-only LIKE / ILIKE direct-read returning false positives.
--
-- Previously the projection text index created a TextSearchQuery with
-- `TextIndexDirectReadMode::Exact` and an empty token list for pure-pattern LIKE
-- (e.g. `LIKE '%foo%'`). The query plan optimiser then *replaced* the original
-- predicate with the virtual column (because Exact mode = full replacement), and the
-- projection reader (which has no fallback path for pattern-only matches) memset the
-- virtual column to 1, yielding every row as a match.
--
-- The fix downgrades pattern-only LIKE/ILIKE to Hint mode so the original predicate
-- is preserved as a post-filter; the projection's all-1 hint still allows the index
-- to prune, but the post-filter applies the actual pattern semantics per row.

DROP TABLE IF EXISTS t_like_postfilter;

CREATE TABLE t_like_postfilter
(
    id UInt32,
    msg String,
    PROJECTION idx INDEX msg TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_like_postfilter VALUES
    (1, 'apple pie'),
    (2, 'banana split'),
    (3, 'apple turnover'),
    (4, 'pumpkin pie');

-- Only rows 1 and 3 contain 'apple'. With the bug, the projection reader would return
-- all 4 rows because the original LIKE was dropped.
SELECT count() FROM t_like_postfilter WHERE msg LIKE '%apple%';

-- 'pie' is in rows 1 and 4.
SELECT count() FROM t_like_postfilter WHERE msg LIKE '%pie%';

-- A pattern that no row matches must still return 0, not a false-positive row count.
SELECT count() FROM t_like_postfilter WHERE msg LIKE '%xyz%';

-- ILIKE pattern-only, same shape.
SELECT count() FROM t_like_postfilter WHERE msg ILIKE '%APPLE%';

DROP TABLE t_like_postfilter;
