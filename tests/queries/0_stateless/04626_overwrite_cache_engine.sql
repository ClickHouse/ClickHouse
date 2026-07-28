DROP TABLE IF EXISTS overwrite_cache;
DROP TABLE IF EXISTS overwrite_cache_small;
DROP TABLE IF EXISTS bad_lookup_alter;

CREATE TABLE overwrite_cache
(
    website_type UInt8,
    user_id UInt64,
    tag LowCardinality(String),
    version UInt64,
    tie UInt64,
    payload String
)
ENGINE = OverwriteCache(version)
KEYS (website_type, user_id, tag)
INDEX (tag), (website_type), (website_type, tag)
SETTINGS
    max_memory_bytes = 10000000,
    equal_version_tiebreak_columns = 'tie';

SHOW CREATE TABLE overwrite_cache FORMAT TSVRaw;

INSERT INTO overwrite_cache VALUES
    (1, 100, 'A', 1, 1, 'old'),
    (2, 200, 'A', 1, 1, 'site-two'),
    (1, 300, 'B', 1, 1, 'tag-b');

INSERT INTO overwrite_cache VALUES (1, 100, 'A', 2, 1, 'new');
INSERT INTO overwrite_cache VALUES (1, 100, 'A', 1, 9, 'stale-version');
INSERT INTO overwrite_cache VALUES (1, 100, 'A', 2, 1, 'new');
INSERT INTO overwrite_cache VALUES (1, 100, 'A', 2, 2, 'tie-winner');
INSERT INTO overwrite_cache VALUES (1, 100, 'A', 2, 1, 'stale-tie');
INSERT INTO overwrite_cache VALUES
    (1, 300, 'C', 1, 1, 'batch-old'),
    (1, 300, 'C', 3, 1, 'batch-new'),
    (1, 300, 'C', 2, 1, 'batch-middle');

SELECT version, tie, payload
FROM overwrite_cache
WHERE website_type = 1 AND user_id = 100 AND tag = 'A';

SELECT website_type, user_id, payload
FROM overwrite_cache
WHERE tag = 'A'
ORDER BY website_type, user_id;

SELECT website_type, user_id, payload
FROM overwrite_cache
WHERE tag = 'A' AND website_type = 1;

SELECT website_type, user_id, payload
FROM overwrite_cache
WHERE tag = 'A' AND website_type != 2;

SELECT payload
FROM overwrite_cache
WHERE website_type = 1 AND user_id IN (100, 999) AND tag = 'A';
SELECT payload FROM overwrite_cache WHERE 1 = website_type AND 100 = user_id AND 'A' = tag;
SELECT payload FROM overwrite_cache WHERE (website_type, user_id, tag) = (1, 100, 'A');
SELECT payload FROM overwrite_cache WHERE (1, 100, 'A') = (website_type, user_id, tag);
SELECT payload FROM overwrite_cache WHERE (website_type, user_id, tag) IN ((1, 100, 'A')) AND length(payload) = 10;
SELECT payload FROM overwrite_cache WHERE (website_type, user_id, tag) IN ((1, 100, 'A')) AND length(payload) = 999;
SELECT payload FROM overwrite_cache WHERE user_id = 100; -- { serverError BAD_ARGUMENTS }
ALTER TABLE overwrite_cache ADD INDEX (user_id);
ALTER TABLE overwrite_cache ADD INDEX IF NOT EXISTS (user_id);
ALTER TABLE overwrite_cache ADD INDEX (user_id); -- { serverError BAD_ARGUMENTS }
SELECT payload FROM overwrite_cache WHERE user_id = 100;
SHOW CREATE TABLE overwrite_cache FORMAT TSVRaw;
ALTER TABLE overwrite_cache DROP INDEX (user_id);
ALTER TABLE overwrite_cache DROP INDEX IF EXISTS (user_id);
ALTER TABLE overwrite_cache DROP INDEX (user_id); -- { serverError BAD_ARGUMENTS }
SELECT payload FROM overwrite_cache WHERE user_id = 100; -- { serverError BAD_ARGUMENTS }
SHOW CREATE TABLE overwrite_cache FORMAT TSVRaw;
SELECT count() FROM overwrite_cache WHERE website_type = 1 AND user_id IN tuple() AND tag = 'A';
SELECT count() FROM overwrite_cache WHERE tag IN tuple();
SELECT payload FROM overwrite_cache WHERE website_type = 1 AND user_id = 300 AND tag = 'C';
SELECT payload FROM overwrite_cache WHERE tag = 'A' OR payload = 'site-two'; -- { serverError BAD_ARGUMENTS }
SELECT payload FROM overwrite_cache WHERE user_id > 100; -- { serverError BAD_ARGUMENTS }

SELECT overwriteCacheGet(
    'overwrite_cache', 'payload', toUInt8(1), toUInt64(100), 'A');
SELECT overwriteCacheGetOrNull(
    'overwrite_cache', 'payload', toUInt8(9), toUInt64(999), 'missing') IS NULL;
SELECT toTypeName(overwriteCacheGetOrNull(
    'overwrite_cache', 'tag', toUInt8(1), toUInt64(100), 'A'));
SELECT
    countIf(overwriteCacheGet('overwrite_cache', 'payload', website_type, user_id, tag) = ''),
    countIf(overwriteCacheGetOrNull('overwrite_cache', 'payload', website_type, user_id, tag) IS NULL)
FROM VALUES(
    'website_type UInt8, user_id UInt64, tag String',
    (1, 100, 'A'),
    (9, 999, 'missing'));

SYSTEM ENABLE FAILPOINT overwrite_cache_throw_during_publish;
INSERT INTO overwrite_cache VALUES
    (1, 500, 'E', 1, 1, 'must-roll-back'),
    (1, 501, 'F', 1, 1, 'must-also-roll-back'); -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT overwrite_cache_throw_during_publish;
SELECT payload FROM overwrite_cache WHERE website_type = 1 AND user_id = 500 AND tag = 'E';
SELECT payload FROM overwrite_cache WHERE website_type = 1 AND user_id = 501 AND tag = 'F';
SELECT payload FROM overwrite_cache WHERE tag = 'E';
SELECT payload FROM overwrite_cache WHERE website_type = 1 AND tag = 'F';
INSERT INTO overwrite_cache VALUES (1, 500, 'E', 2, 1, 'after-failure');
SELECT payload FROM overwrite_cache WHERE website_type = 1 AND user_id = 500 AND tag = 'E';
SELECT payload FROM overwrite_cache WHERE tag = 'E';
SYSTEM ENABLE FAILPOINT overwrite_cache_throw_during_publish;
INSERT INTO overwrite_cache VALUES
    (1, 100, 'A', 4, 1, 'replacement-must-roll-back'),
    (1, 300, 'C', 4, 1, 'other-replacement-must-roll-back'); -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT overwrite_cache_throw_during_publish;
SELECT payload FROM overwrite_cache WHERE website_type = 1 AND user_id = 100 AND tag = 'A';
SELECT payload FROM overwrite_cache WHERE website_type = 1 AND user_id = 300 AND tag = 'C';
SELECT count() FROM overwrite_cache; -- { serverError BAD_ARGUMENTS }
INSERT INTO overwrite_cache VALUES (1, 201, 'A', 1, 1, 'indexed-row');
SELECT payload FROM overwrite_cache WHERE tag = 'A' ORDER BY user_id;
SELECT payload FROM overwrite_cache WHERE website_type = 1 AND user_id = 201 AND tag = 'A';

-- The rows are persisted, so a detach and attach replays the log instead of emptying the cache.
-- 04633_overwrite_cache_persistence covers what replay has to reproduce.
DETACH TABLE overwrite_cache;
ATTACH TABLE overwrite_cache;
SELECT payload
FROM overwrite_cache
WHERE website_type = 1 AND user_id = 100 AND tag = 'A';

CREATE TABLE overwrite_cache_small
(
    key UInt64,
    version UInt64,
    payload String
)
ENGINE = OverwriteCache(version)
KEYS (key)
SETTINGS max_memory_bytes = 131072;

INSERT INTO overwrite_cache_small VALUES (1, 1, repeat('x', 200000)), (2, 1, repeat('y', 200000)); -- { serverError MEMORY_LIMIT_EXCEEDED }
SELECT payload FROM overwrite_cache_small WHERE key = 1;

INSERT INTO overwrite_cache_small VALUES (1, 1, 'same-version-a');
INSERT INTO overwrite_cache_small VALUES (1, 1, 'same-version-b');
SELECT payload FROM overwrite_cache_small WHERE key = 1;

TRUNCATE TABLE overwrite_cache_small;
SELECT payload FROM overwrite_cache_small WHERE key = 1;
SELECT total_rows, total_bytes
FROM system.tables
WHERE database = currentDatabase() AND name = 'overwrite_cache_small';

CREATE TABLE bad_keys (key UInt64, version UInt64) ENGINE = Memory KEYS (key); -- { serverError BAD_ARGUMENTS }
CREATE TABLE bad_keys_syntax (key UInt64, version UInt64) ENGINE = OverwriteCache(version) KEYS key SETTINGS max_memory_bytes = 10000; -- { clientError SYNTAX_ERROR }
CREATE TABLE missing_keys (key UInt64, version UInt64) ENGINE = OverwriteCache(version); -- { serverError BAD_ARGUMENTS }
CREATE TABLE version_in_keys (key UInt64, version UInt64) ENGINE = OverwriteCache(version) KEYS (key, version); -- { serverError BAD_ARGUMENTS }
CREATE TABLE bad_lookup_column (key UInt64, other UInt64, version UInt64) ENGINE = OverwriteCache(version) KEYS (key) INDEX (other); -- { serverError BAD_ARGUMENTS }
CREATE TABLE duplicate_lookup (key UInt64, other UInt64, version UInt64) ENGINE = OverwriteCache(version) KEYS (key, other) INDEX (key), (key); -- { serverError BAD_ARGUMENTS }
CREATE TABLE duplicate_lookup_column (key UInt64, other UInt64, version UInt64) ENGINE = OverwriteCache(version) KEYS (key, other) INDEX (key, key); -- { serverError BAD_ARGUMENTS }
CREATE TABLE canonical_duplicate_lookup (key UInt64, other UInt64, version UInt64) ENGINE = OverwriteCache(version) KEYS (key, other) INDEX (key, other), (other, key); -- { serverError BAD_ARGUMENTS }
CREATE TABLE primary_lookup (key UInt64, other UInt64, version UInt64) ENGINE = OverwriteCache(version) KEYS (key, other) INDEX (key, other); -- { serverError BAD_ARGUMENTS }
CREATE TABLE bad_lookup_engine (key UInt64, version UInt64) ENGINE = Memory INDEX (key); -- { serverError BAD_ARGUMENTS }
ALTER TABLE overwrite_cache ADD INDEX (user_id) FIRST; -- { serverError BAD_ARGUMENTS }
ALTER TABLE overwrite_cache ADD INDEX (user_id) AFTER tag; -- { serverError BAD_ARGUMENTS }
CREATE TABLE bad_lookup_alter (key UInt64) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE bad_lookup_alter ADD INDEX (key); -- { serverError BAD_ARGUMENTS }
ALTER TABLE bad_lookup_alter DROP INDEX (key); -- { serverError BAD_ARGUMENTS }
DROP TABLE bad_lookup_alter;
CREATE TABLE old_lookup_settings (key UInt64, version UInt64) ENGINE = OverwriteCache(version) KEYS (key) SETTINGS secondary_index_columns = 'key'; -- { serverError UNKNOWN_SETTING }

DROP TABLE overwrite_cache_small;
DROP TABLE overwrite_cache;
