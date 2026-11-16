-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Tests that the query condition cache is populated for granules eliminated by any conjunct of a WHERE, not just by the
-- last one. With `query_plan_merge_filters` (the default), `FilterStep` splits an AND chain into one `FilterTransform`
-- per atom. A granule fully eliminated by an atom other than the residual used to be lost: the atom carried no cache
-- condition, and its empty chunk - which holds the `MarkRangesInfo` identifying the granule - was dropped before the
-- residual transform could record it.

set parallel_replicas_local_plan=1;

SET enable_analyzer = 1;

-- Keep the WHERE in a `FilterStep` so the AND chain split is exercised, and make sure nothing else prunes the granules.
SET optimize_move_to_prewhere = 0;
SET query_plan_optimize_prewhere = 0;
SET query_plan_merge_filters = 1;
SET use_query_condition_cache = 1;

SELECT '--- two conjuncts, numeric only';

SYSTEM CLEAR QUERY CONDITION CACHE;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (a Int64, b Int64, c Int64) ENGINE = MergeTree ORDER BY a
SETTINGS add_minmax_index_for_numeric_columns = 0;

-- Query condition cache entries are keyed by part, so a background merge between the two runs would
-- invalidate the primed entries and make the hit counts flaky.
SYSTEM STOP MERGES tab;

-- 1 mio rows sounds like a lot but the QCC doesn't cache anything for less data
INSERT INTO tab SELECT number, number, 1_000_000 - number FROM numbers(1_000_000);

-- `b < 500_000` eliminates the granules in the upper half of the table, `c < 500_000` those in the lower half, so every
-- granule is eliminated by exactly one of the two conjuncts. The splitter takes the left-most atom, so `b < 500_000`
-- becomes a separate transform and `c < 500_000` the residual: without the fix only the lower half is recorded.

SELECT count(*) FROM tab WHERE b < 500_000 AND c < 500_000 SETTINGS log_comment = 'and_chain_split_first_run';

SYSTEM FLUSH LOGS query_log;
SELECT
    ProfileEvents['QueryConditionCacheHits'],
    ProfileEvents['QueryConditionCacheMisses'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'and_chain_split_first_run'
ORDER BY event_time_microseconds;

-- The second run must skip every granule, so `SelectedMarks` is 0. Before the fix only the residual conjunct's half was
-- cached and about half of the marks were read again.
SELECT count(*) FROM tab WHERE b < 500_000 AND c < 500_000 SETTINGS log_comment = 'and_chain_split_second_run';

SYSTEM FLUSH LOGS query_log;
SELECT
    ProfileEvents['SelectedMarks'],
    ProfileEvents['QueryConditionCacheHits'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'and_chain_split_second_run'
ORDER BY event_time_microseconds;

DROP TABLE tab;

SELECT '--- three conjuncts, including a String column';

-- The shape that regressed in practice: a `String` conjunct of the `status NOT IN (...)` kind sitting in the middle of
-- the chain, i.e. neither the left-most atom nor the residual.

SYSTEM CLEAR QUERY CONDITION CACHE;

DROP TABLE IF EXISTS tab_str;

CREATE TABLE tab_str (a Int64, b Int64, c Int64, s String) ENGINE = MergeTree ORDER BY a
SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_string_columns = 0;

SYSTEM STOP MERGES tab_str;

INSERT INTO tab_str
SELECT number, number, number, multiIf(number < 333_333, 'alpha', number < 666_666, 'beta', 'gamma')
FROM numbers(1_000_000);

-- Each conjunct is false on exactly one third of the table and true on the other two, so every granule is eliminated by
-- exactly one of them: `b >= 333_333` kills the first third, `s NOT IN (...)` the second, `c < 666_666` the third. The
-- splitter peels the left-most atoms in order, so the `String` conjunct becomes the middle transform and `c < 666_666`
-- the residual. Before the fix only the last third - the residual's own - was recorded.

SELECT count(*) FROM tab_str
WHERE b >= 333_333 AND s NOT IN ('beta', 'delta') AND c < 666_666
SETTINGS log_comment = 'and_chain_split_string_first_run';

SYSTEM FLUSH LOGS query_log;
SELECT
    ProfileEvents['QueryConditionCacheHits'],
    ProfileEvents['QueryConditionCacheMisses'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'and_chain_split_string_first_run'
ORDER BY event_time_microseconds;

SELECT count(*) FROM tab_str
WHERE b >= 333_333 AND s NOT IN ('beta', 'delta') AND c < 666_666
SETTINGS log_comment = 'and_chain_split_string_second_run';

SYSTEM FLUSH LOGS query_log;
SELECT
    ProfileEvents['SelectedMarks'],
    ProfileEvents['QueryConditionCacheHits'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'and_chain_split_string_second_run'
ORDER BY event_time_microseconds;

DROP TABLE tab_str;
