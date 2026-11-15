-- `DETACH` destroys the storage and `ATTACH` builds it again, which is the same path a restart takes:
-- the log is replayed and the primary index and the lookup postings are rebuilt from the segments.

DROP TABLE IF EXISTS overwrite_cache_persistence;

CREATE TABLE overwrite_cache_persistence
(
    website_type UInt8,
    user_id UInt64,
    tag LowCardinality(String),
    version DateTime64(3),
    source_sequence UInt64,
    value String
)
ENGINE = OverwriteCache(version)
KEYS (website_type, user_id, tag)
INDEX (tag), (website_type)
SETTINGS equal_version_tiebreak_columns = 'source_sequence';

INSERT INTO overwrite_cache_persistence VALUES (1, 42, 'risk', toDateTime64('2026-01-01 00:00:00.000', 3), 1, 'first');
INSERT INTO overwrite_cache_persistence VALUES (1, 43, 'risk', toDateTime64('2026-01-01 00:00:00.000', 3), 1, 'second');
INSERT INTO overwrite_cache_persistence VALUES (2, 44, 'vip', toDateTime64('2026-01-01 00:00:00.000', 3), 1, 'third');

SELECT 'before detach', count() FROM overwrite_cache_persistence WHERE tag IN ('risk', 'vip');

DETACH TABLE overwrite_cache_persistence;
ATTACH TABLE overwrite_cache_persistence;

SELECT 'after attach', count() FROM overwrite_cache_persistence WHERE tag IN ('risk', 'vip');
SELECT 'primary lookup', value FROM overwrite_cache_persistence WHERE website_type = 1 AND user_id = 42 AND tag = 'risk';
SELECT 'index lookup', user_id, value FROM overwrite_cache_persistence WHERE website_type = 1 ORDER BY user_id;

-- A replacement published before the detach has to be the row that comes back, and the row it replaced
-- must not: the segment holding it is retired and its file is removed.
INSERT INTO overwrite_cache_persistence VALUES (1, 42, 'risk', toDateTime64('2026-01-02 00:00:00.000', 3), 1, 'replaced');

DETACH TABLE overwrite_cache_persistence;
ATTACH TABLE overwrite_cache_persistence;

SELECT 'replacement survives', value, version FROM overwrite_cache_persistence WHERE website_type = 1 AND user_id = 42 AND tag = 'risk';
SELECT 'rows after replacement', count() FROM overwrite_cache_persistence WHERE tag IN ('risk', 'vip');

-- A lower version stays ignored after the reload, so replay does not reorder winner selection.
INSERT INTO overwrite_cache_persistence VALUES (1, 42, 'risk', toDateTime64('2025-01-01 00:00:00.000', 3), 1, 'stale');

DETACH TABLE overwrite_cache_persistence;
ATTACH TABLE overwrite_cache_persistence;

SELECT 'lower version still ignored', value FROM overwrite_cache_persistence WHERE website_type = 1 AND user_id = 42 AND tag = 'risk';

-- A deleted key must not come back. Its row is dead inside a segment that nothing supersedes, so the
-- deletion itself has to be part of the log.
DELETE FROM overwrite_cache_persistence WHERE website_type = 1 AND user_id = 43 AND tag = 'risk';

DETACH TABLE overwrite_cache_persistence;
ATTACH TABLE overwrite_cache_persistence;

SELECT 'deleted key stays deleted', count() FROM overwrite_cache_persistence WHERE website_type = 1 AND user_id = 43 AND tag = 'risk';
SELECT 'rows after delete', count() FROM overwrite_cache_persistence WHERE tag IN ('risk', 'vip');

-- A delete is not a version floor, and replay must not turn it into one: the reinserted row wins even
-- though its version is below the one that was deleted.
INSERT INTO overwrite_cache_persistence VALUES (1, 43, 'risk', toDateTime64('2020-01-01 00:00:00.000', 3), 1, 'resurrected');

DETACH TABLE overwrite_cache_persistence;
ATTACH TABLE overwrite_cache_persistence;

SELECT 'resurrected row survives', value FROM overwrite_cache_persistence WHERE website_type = 1 AND user_id = 43 AND tag = 'risk';

-- An index added after the data was written is rebuilt from the segments, so it is complete afterwards.
ALTER TABLE overwrite_cache_persistence ADD INDEX (user_id);

DETACH TABLE overwrite_cache_persistence;
ATTACH TABLE overwrite_cache_persistence;

SELECT 'added index rebuilt', value FROM overwrite_cache_persistence WHERE user_id = 44;

TRUNCATE TABLE overwrite_cache_persistence;

DETACH TABLE overwrite_cache_persistence;
ATTACH TABLE overwrite_cache_persistence;

SELECT 'truncate is persisted', count() FROM overwrite_cache_persistence WHERE tag IN ('risk', 'vip');

DROP TABLE overwrite_cache_persistence;

-- `persist_mode = 'none'` keeps the behaviour of a purely in-memory table.
DROP TABLE IF EXISTS overwrite_cache_volatile;

CREATE TABLE overwrite_cache_volatile
(
    key UInt64,
    version UInt64,
    value String
)
ENGINE = OverwriteCache(version)
KEYS (key)
SETTINGS persist_mode = 'none';

INSERT INTO overwrite_cache_volatile VALUES (1, 1, 'gone');
SELECT 'volatile before detach', count() FROM overwrite_cache_volatile WHERE key = 1;

DETACH TABLE overwrite_cache_volatile;
ATTACH TABLE overwrite_cache_volatile;

SELECT 'volatile after attach', count() FROM overwrite_cache_volatile WHERE key = 1;

DROP TABLE overwrite_cache_volatile;

-- `sync` waits for the publication to be durable before the `INSERT` returns.
DROP TABLE IF EXISTS overwrite_cache_sync;

CREATE TABLE overwrite_cache_sync
(
    key UInt64,
    version UInt64,
    value String
)
ENGINE = OverwriteCache(version)
KEYS (key)
SETTINGS persist_mode = 'sync';

INSERT INTO overwrite_cache_sync VALUES (1, 1, 'durable');
INSERT INTO overwrite_cache_sync VALUES (1, 2, 'durable again');

DETACH TABLE overwrite_cache_sync;
ATTACH TABLE overwrite_cache_sync;

SELECT 'sync mode', value FROM overwrite_cache_sync WHERE key = 1;

DROP TABLE overwrite_cache_sync;

-- An unknown mode is rejected at `CREATE` time rather than falling back to a default.
CREATE TABLE overwrite_cache_bad_mode
(
    key UInt64,
    version UInt64
)
ENGINE = OverwriteCache(version)
KEYS (key)
SETTINGS persist_mode = 'eventually'; -- { serverError BAD_ARGUMENTS }
