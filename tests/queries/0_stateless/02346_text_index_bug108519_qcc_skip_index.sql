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

-- Two indexes on different columns, each producing the same kind of false negative. Ignoring a
-- different index name yields a different effective set, hence a different profiled key, so an entry
-- written while ignoring 'idx_b' (i.e. running 'idx_a') is not served to a query ignoring 'idx_a'
-- (running 'idx_b'). The names are folded into the key as fixed-width definition hashes, so distinct
-- sets cannot collide. Both answers are the correct row-level 1.
DROP TABLE IF EXISTS tab_two;
CREATE TABLE tab_two
(
    a String,
    b String,
    INDEX idx_a a TYPE text(tokenizer = splitByNonAlpha, preprocessor = replaceAll(a, ' ', '')),
    INDEX idx_b b TYPE text(tokenizer = splitByNonAlpha, preprocessor = replaceAll(b, ' ', ''))
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO tab_two VALUES ('zzz', 'zzz'), ('a b', 'a b');

-- Run with idx_a active (idx_b ignored): idx_a drops the 'a b' granule for hasToken(a, 'a').
SELECT 'two_idx_populate_a', count() FROM tab_two WHERE hasToken(a, 'a')
SETTINGS use_query_condition_cache = 1, use_skip_indexes_on_data_read = 0, ignore_data_skipping_indices = 'idx_b';

-- Same predicate, but now idx_a is the ignored one (idx_b active): a different effective set, so the
-- entry above must not be consulted. Correct row-level answer is 1.
SELECT 'two_idx_ignore_a', count() FROM tab_two WHERE hasToken(a, 'a')
SETTINGS use_query_condition_cache = 1, ignore_data_skipping_indices = 'idx_a';

DROP TABLE tab_two;

-- use_skip_indexes_for_disjunctions changes the exclusions the same indexes produce for OR
-- predicates (the combined-index pruning only runs when it is on), so it is part of the effective
-- profile. The divergence here is not visible as a different row count: the per-column text index
-- still filters the 'a b' granule at data-read time regardless of the disjunction mode, so the count
-- is the same either way. What must differ is the QCC consultation: an entry written under the
-- disjunction-enabled profile must NOT be consulted by a query that disabled the optimization, while
-- a query with the same profile must still reuse it (so the combined-index exclusions are not
-- re-evaluated). Assert that via the QueryConditionCacheHits/Misses profile events.
DROP TABLE IF EXISTS tab_disj;
CREATE TABLE tab_disj
(
    s String,
    n UInt64,
    INDEX idx_s s TYPE text(tokenizer = splitByNonAlpha, preprocessor = replaceAll(s, ' ', '')),
    INDEX idx_n n TYPE minmax
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO tab_disj VALUES ('zzz', 1), ('a b', 2);
SYSTEM DROP QUERY CONDITION CACHE;

-- Populate under the disjunction-enabled profile.
SELECT count() FROM tab_disj WHERE hasToken(s, 'a') OR n = 999
SETTINGS use_query_condition_cache = 1, use_skip_indexes_for_disjunctions = 1, use_skip_indexes_on_data_read = 0, log_comment = '02346_qcc_disj_populate' FORMAT Null;

-- Same predicate, disjunction optimization disabled: a different effective profile, so this must miss.
SELECT count() FROM tab_disj WHERE hasToken(s, 'a') OR n = 999
SETTINGS use_query_condition_cache = 1, use_skip_indexes_for_disjunctions = 0, use_skip_indexes_on_data_read = 0, log_comment = '02346_qcc_disj_off' FORMAT Null;

-- Same predicate, same (enabled) profile: must hit, so cached combined-index exclusions are reused.
SELECT count() FROM tab_disj WHERE hasToken(s, 'a') OR n = 999
SETTINGS use_query_condition_cache = 1, use_skip_indexes_for_disjunctions = 1, use_skip_indexes_on_data_read = 0, log_comment = '02346_qcc_disj_same' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT 'disj_off_consults_entry', ProfileEvents['QueryConditionCacheHits']
FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '02346_qcc_disj_off'
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT 'disj_same_reuses_entry', ProfileEvents['QueryConditionCacheHits']
FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '02346_qcc_disj_same'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE tab_disj;
