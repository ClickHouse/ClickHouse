-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas: this is a single-node text-index test; parallel replicas relocate
-- index analysis and are irrelevant to the QCC read-gate behavior under test.

-- Regression test for #108519: the query condition cache (QCC) was consulted by a query
-- that explicitly disabled skip indexes (use_skip_indexes = 0), even though the cached
-- verdict had been produced WITH a skip index. When the skip index legitimately diverges
-- from the row-level predicate (here a text index with a preprocessor that strips spaces),
-- the cached "no match" mark verdict is a false negative for the row-level predicate, so
-- the use_skip_indexes = 0 query dropped a granule it should have kept and under-counted.
--
-- Ingredients, pinned per query so CI randomization cannot disable them:
--   * allow_experimental_full_text_index = 1   - the text index used as the trigger.
--   * allow_experimental_analyzer = 1           - QCC is analyzer-only.
--   * use_query_condition_cache = 1             - the cache that gets poisoned.
--   * use_skip_indexes_on_data_read = 0         - runs the text index at index-analysis
--                                                 time, so its dropped mark is persisted to
--                                                 the QCC under the bare WHERE hash.
-- The table uses a fresh per-test database (unique UUID), so the QCC starts empty for these
-- keys without a server-global SYSTEM DROP QUERY CONDITION CACHE (keeps the test parallel-safe).

SET allow_experimental_full_text_index = 1;
SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS tab;

-- preprocessor strips spaces: row 'a b' is indexed as token 'ab', so the text index does
-- NOT match hasToken(s, 'a') even though the row-level predicate does (it splits on space).
CREATE TABLE tab
(
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, preprocessor = replaceAll(s, ' ', ''))
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

INSERT INTO tab VALUES ('zzz'), ('a b');

-- Populate the QCC via the index path. The text index drops the 'a b' granule (false
-- negative), and that exclusion is cached under the WHERE-condition hash.
SELECT 'index_path', count() FROM tab WHERE hasToken(s, 'a')
SETTINGS use_query_condition_cache = 1, use_skip_indexes_on_data_read = 0;

-- Same predicate with skip indexes disabled. This must NOT consult the skip-index-derived
-- cache entry. Correct row-level answer is 1 ('a b' contains token 'a'). Before the fix this
-- returned 0.
SELECT 'skip_indexes_off', count() FROM tab WHERE hasToken(s, 'a')
SETTINGS use_query_condition_cache = 1, use_skip_indexes = 0;

-- Sanity: cache-off path agrees.
SELECT 'no_cache', count() FROM tab WHERE hasToken(s, 'a')
SETTINGS use_query_condition_cache = 0, use_skip_indexes = 0;

DROP TABLE tab;
