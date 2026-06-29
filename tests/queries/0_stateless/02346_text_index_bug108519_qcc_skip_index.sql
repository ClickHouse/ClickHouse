-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas: this is a single-node test; parallel replicas relocate index
-- analysis and the query condition cache writes, so the granule-level checks below do not apply.

-- Regression test for #108519: the query condition cache (QCC) stored a skip-index-derived
-- exclusion under the bare WHERE-condition hash, so a later query that ran a different set of
-- skip indexes (use_skip_indexes = 0, or ignore_data_skipping_indices) consulted it. When the
-- skip index legitimately diverges from the row-level predicate (here a text index with a
-- preprocessor that strips spaces), the cached "no match" verdict is a false negative for the
-- row-level predicate, so the second query dropped a granule it should have kept and under-counted.
--
-- The fix keys skip-index-derived QCC entries by the effective skip-index profile, so they are
-- only consulted by a query that ran the same indexes. Row-level entries keep the bare hash and
-- stay readable under any profile (the pure-QCC learned-index case, checked at the end).

SET allow_experimental_full_text_index = 1;
SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS tab;

-- preprocessor strips spaces: row 'a b' is indexed as token 'ab', so the text index does NOT
-- match hasToken(s, 'a') even though the row-level predicate does (it splits on space).
CREATE TABLE tab
(
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, preprocessor = replaceAll(s, ' ', ''))
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

INSERT INTO tab VALUES ('zzz'), ('a b');

-- Populate the QCC via the index path. The text index drops the 'a b' granule (false negative),
-- and that exclusion is cached under the profiled (skip-index) key.
SELECT 'index_path', count() FROM tab WHERE hasToken(s, 'a')
SETTINGS use_query_condition_cache = 1, use_skip_indexes_on_data_read = 0;

-- Same predicate with skip indexes disabled must NOT consult the skip-index-derived entry.
-- Correct row-level answer is 1 ('a b' contains token 'a'). Before the fix this returned 0.
SELECT 'skip_indexes_off', count() FROM tab WHERE hasToken(s, 'a')
SETTINGS use_query_condition_cache = 1, use_skip_indexes = 0;

-- Sanity: cache-off path agrees.
SELECT 'no_cache', count() FROM tab WHERE hasToken(s, 'a')
SETTINGS use_query_condition_cache = 0, use_skip_indexes = 0;

-- Re-populate via the index path, then rerun ignoring the named index. ignore_data_skipping_indices
-- = 'idx' disables 'idx' while use_skip_indexes stays true, a different effective profile, so the
-- skip-index entry must not be consulted. Correct answer is 1; before the fix this returned 0.
SELECT 'index_path', count() FROM tab WHERE hasToken(s, 'a')
SETTINGS use_query_condition_cache = 1, use_skip_indexes_on_data_read = 0;

SELECT 'ignore_index', count() FROM tab WHERE hasToken(s, 'a')
SETTINGS use_query_condition_cache = 1, ignore_data_skipping_indices = 'idx';

-- The skip-index path keeps pruning for a query with the same profile (the cached exclusion is
-- still reused, so this stays 0). Confirms the profiled key did not disable skip-index caching.
SELECT 'index_path_still_prunes', count() FROM tab WHERE hasToken(s, 'a')
SETTINGS use_query_condition_cache = 1, use_skip_indexes_on_data_read = 0;

DROP TABLE tab;

-- A row-level QCC entry (no skip index involved) must still be consulted when skip indexes are
-- disabled: the fix only profiles skip-index-derived entries, so the pure-QCC learned-index case
-- keeps working. Populate via the row-level WHERE path with use_skip_indexes = 0, then assert a
-- granule was pruned on reuse via EXPLAIN. max_block_size = 8 makes each non-matching granule a
-- fully filtered chunk so FilterTransform records it.
DROP TABLE IF EXISTS rl;
CREATE TABLE rl (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO rl SELECT number, number FROM numbers(1000);

SELECT 'rowlevel_populate', count() FROM rl WHERE v = 500
SETTINGS use_query_condition_cache = 1, use_skip_indexes = 0, max_block_size = 8, max_threads = 1;

SELECT 'rowlevel_qcc_prunes', countIf(explain LIKE '%Granules: 1/125%') FROM (
    EXPLAIN indexes = 1 SELECT count() FROM rl WHERE v = 500
    SETTINGS use_query_condition_cache = 1, use_skip_indexes = 0
);

DROP TABLE rl;
