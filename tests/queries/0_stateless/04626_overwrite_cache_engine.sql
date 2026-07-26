DROP TABLE IF EXISTS overwrite_cache;
DROP TABLE IF EXISTS overwrite_cache_small;

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
SETTINGS
    max_memory_bytes = 10000000,
    equal_version_tiebreak_columns = 'tie',
    secondary_index_columns = 'tag',
    secondary_index_segment_column = 'website_type',
    max_secondary_index_rows = 2;

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
SELECT count() FROM overwrite_cache WHERE website_type = 1 AND user_id IN tuple() AND tag = 'A';
SELECT count() FROM overwrite_cache WHERE tag IN tuple();
SELECT payload FROM overwrite_cache WHERE website_type = 1 AND user_id = 300 AND tag = 'C';

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

INSERT INTO overwrite_cache VALUES (1, 100, 'A', 2, 2, 'conflict'); -- { serverError BAD_ARGUMENTS }
INSERT INTO overwrite_cache VALUES
    (1, 400, 'D', 1, 1, 'conflict-a'),
    (1, 400, 'D', 1, 1, 'conflict-b'); -- { serverError BAD_ARGUMENTS }
SELECT payload FROM overwrite_cache WHERE website_type = 1 AND user_id = 400 AND tag = 'D';
INSERT INTO overwrite_cache VALUES
    (1, 100, 'A', 3, 1, 'must-not-commit'),
    (1, 300, 'B', 1, 1, 'conflict-existing'); -- { serverError BAD_ARGUMENTS }
SELECT payload FROM overwrite_cache WHERE website_type = 1 AND user_id = 100 AND tag = 'A';
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
INSERT INTO overwrite_cache VALUES (1, 201, 'A', 1, 1, 'index-limit');
SELECT payload FROM overwrite_cache WHERE tag = 'A'; -- { serverError MEMORY_LIMIT_EXCEEDED }
SELECT payload FROM overwrite_cache WHERE website_type = 1 AND user_id = 201 AND tag = 'A';

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
SETTINGS max_memory_bytes = 800;

INSERT INTO overwrite_cache_small VALUES (1, 1, repeat('x', 100)), (2, 1, repeat('y', 100)); -- { serverError MEMORY_LIMIT_EXCEEDED }
SELECT payload FROM overwrite_cache_small WHERE key = 1;

INSERT INTO overwrite_cache_small VALUES (1, 1, 'same-version-a');
INSERT INTO overwrite_cache_small VALUES (1, 1, 'same-version-b'); -- { serverError BAD_ARGUMENTS }
SELECT payload FROM overwrite_cache_small WHERE key = 1;

TRUNCATE TABLE overwrite_cache_small;
SELECT payload FROM overwrite_cache_small WHERE key = 1;

CREATE TABLE bad_keys (key UInt64, version UInt64) ENGINE = Memory KEYS (key); -- { serverError BAD_ARGUMENTS }
CREATE TABLE bad_keys_syntax (key UInt64, version UInt64) ENGINE = OverwriteCache(version) KEYS key SETTINGS max_memory_bytes = 10000; -- { clientError SYNTAX_ERROR }
CREATE TABLE missing_keys (key UInt64, version UInt64) ENGINE = OverwriteCache(version) SETTINGS max_memory_bytes = 10000; -- { serverError BAD_ARGUMENTS }
CREATE TABLE version_in_keys (key UInt64, version UInt64) ENGINE = OverwriteCache(version) KEYS (key, version) SETTINGS max_memory_bytes = 10000; -- { serverError BAD_ARGUMENTS }
CREATE TABLE bad_secondary (key UInt64, other UInt64, version UInt64) ENGINE = OverwriteCache(version) KEYS (key) SETTINGS max_memory_bytes = 10000, secondary_index_columns = 'other', max_secondary_index_rows = 10; -- { serverError BAD_ARGUMENTS }
CREATE TABLE duplicate_secondary_segment (key UInt64, version UInt64) ENGINE = OverwriteCache(version) KEYS (key) SETTINGS max_memory_bytes = 10000, secondary_index_columns = 'key', secondary_index_segment_column = 'key', max_secondary_index_rows = 10; -- { serverError BAD_ARGUMENTS }

DROP TABLE overwrite_cache_small;
DROP TABLE overwrite_cache;
