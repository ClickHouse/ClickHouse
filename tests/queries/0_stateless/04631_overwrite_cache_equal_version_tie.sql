-- Rows that tie on the version and tie-break columns never replace the row already stored, even when
-- their payload differs. A duplicate produced by a faulty upstream query must not fail the insert.

DROP TABLE IF EXISTS overwrite_cache_tie;

CREATE TABLE overwrite_cache_tie
(
    user_id UInt64,
    version UInt64,
    tie UInt64,
    payload String
)
ENGINE = OverwriteCache(version)
KEYS (user_id)
SETTINGS equal_version_tiebreak_columns = 'tie';

SELECT '-- a tie across separate inserts keeps the stored row';
INSERT INTO overwrite_cache_tie VALUES (1, 5, 1, 'first');
INSERT INTO overwrite_cache_tie VALUES (1, 5, 1, 'second');
SELECT payload FROM overwrite_cache_tie WHERE user_id = 1;

SELECT '-- an identical row is still a no-op';
INSERT INTO overwrite_cache_tie VALUES (1, 5, 1, 'first');
SELECT payload FROM overwrite_cache_tie WHERE user_id = 1;

SELECT '-- a tie inside one block keeps the earlier row';
INSERT INTO overwrite_cache_tie VALUES (2, 5, 1, 'earlier'), (2, 5, 1, 'later');
SELECT payload FROM overwrite_cache_tie WHERE user_id = 2;

SELECT '-- a greater tie-break and a greater version still win over a tie';
INSERT INTO overwrite_cache_tie VALUES (1, 5, 2, 'tie-winner');
SELECT payload FROM overwrite_cache_tie WHERE user_id = 1;
INSERT INTO overwrite_cache_tie VALUES (1, 6, 1, 'version-winner');
SELECT payload FROM overwrite_cache_tie WHERE user_id = 1;

DROP TABLE overwrite_cache_tie;

SELECT '-- the same holds without tie-break columns';

CREATE TABLE overwrite_cache_no_tiebreak
(
    user_id UInt64,
    version UInt64,
    payload String
)
ENGINE = OverwriteCache(version)
KEYS (user_id);

INSERT INTO overwrite_cache_no_tiebreak VALUES (1, 5, 'first');
INSERT INTO overwrite_cache_no_tiebreak VALUES (1, 5, 'second');
SELECT payload FROM overwrite_cache_no_tiebreak WHERE user_id = 1;

DROP TABLE overwrite_cache_no_tiebreak;

SELECT '-- ignored rows are counted';

CREATE TABLE overwrite_cache_counted
(
    user_id UInt64,
    version UInt64,
    payload String
)
ENGINE = OverwriteCache(version)
KEYS (user_id);

INSERT INTO overwrite_cache_counted VALUES (1, 5, 'first');

SET log_queries = 1;
SET async_insert = 0;
INSERT INTO overwrite_cache_counted VALUES (1, 5, 'a'), (1, 5, 'b'), (2, 5, 'c'), (2, 5, 'd');
SET log_queries = 0;

SYSTEM FLUSH LOGS query_log;

-- Three rows lose a tie: one inside the block for each key, and the survivor for key 1 against the stored row.
SELECT ProfileEvents['OverwriteCacheEqualVersionTies']
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query LIKE 'INSERT INTO overwrite_cache_counted VALUES%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE overwrite_cache_counted;
