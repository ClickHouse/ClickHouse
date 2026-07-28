-- `DELETE` accepts exactly the predicates a `SELECT` accepts: a complete `KEYS` tuple, or one or more
-- declared lookup indexes. Anything that would need a scan is rejected instead.

DROP TABLE IF EXISTS overwrite_cache_delete;

CREATE TABLE overwrite_cache_delete
(
    website_type UInt8,
    user_id UInt64,
    tag LowCardinality(String),
    version UInt64,
    payload String
)
ENGINE = OverwriteCache(version)
KEYS (website_type, user_id, tag)
INDEX (tag), (website_type);

INSERT INTO overwrite_cache_delete VALUES
    (1, 100, 'A', 1, 'a-one'),
    (1, 200, 'A', 1, 'a-two'),
    (1, 300, 'B', 1, 'b-one'),
    (2, 400, 'C', 1, 'c-one');

SELECT '-- a complete KEYS predicate deletes one row';
DELETE FROM overwrite_cache_delete WHERE website_type = 1 AND user_id = 100 AND tag = 'A';
SELECT payload FROM overwrite_cache_delete WHERE website_type = 1 AND user_id = 100 AND tag = 'A';
SELECT payload FROM overwrite_cache_delete WHERE tag = 'A';

SELECT '-- a lookup-index predicate deletes every matching row';
DELETE FROM overwrite_cache_delete WHERE tag = 'A';
SELECT payload FROM overwrite_cache_delete WHERE tag = 'A';
SELECT payload FROM overwrite_cache_delete ORDER BY payload SETTINGS max_threads = 1; -- { serverError BAD_ARGUMENTS }
SELECT payload FROM overwrite_cache_delete WHERE website_type = 1 ORDER BY payload;

SELECT '-- an extra predicate narrows an indexed delete';
DELETE FROM overwrite_cache_delete WHERE website_type = 1 AND payload = 'nothing-matches';
SELECT payload FROM overwrite_cache_delete WHERE website_type = 1 ORDER BY payload;
DELETE FROM overwrite_cache_delete WHERE website_type = 1 AND payload = 'b-one';
SELECT payload FROM overwrite_cache_delete WHERE website_type = 1 ORDER BY payload;

SELECT '-- rows behind another index value are untouched';
SELECT payload FROM overwrite_cache_delete WHERE tag = 'C';

SELECT '-- a predicate that needs a scan is rejected';
DELETE FROM overwrite_cache_delete WHERE payload = 'c-one'; -- { serverError BAD_ARGUMENTS }
DELETE FROM overwrite_cache_delete WHERE 1; -- { serverError BAD_ARGUMENTS }
DELETE FROM overwrite_cache_delete WHERE user_id = 400; -- { serverError BAD_ARGUMENTS }
SELECT payload FROM overwrite_cache_delete WHERE tag = 'C';

SELECT '-- deleting an absent key is a no-op';
DELETE FROM overwrite_cache_delete WHERE website_type = 9 AND user_id = 999 AND tag = 'missing';
SELECT total_rows FROM system.tables WHERE database = currentDatabase() AND name = 'overwrite_cache_delete';

SELECT '-- a deleted key can be inserted again, at any version';
INSERT INTO overwrite_cache_delete VALUES (1, 100, 'A', 1, 'a-one-again');
SELECT payload FROM overwrite_cache_delete WHERE website_type = 1 AND user_id = 100 AND tag = 'A';
SELECT payload FROM overwrite_cache_delete WHERE tag = 'A';
SELECT total_rows FROM system.tables WHERE database = currentDatabase() AND name = 'overwrite_cache_delete';

SELECT '-- a resurrected row is replaced by a greater version as usual';
INSERT INTO overwrite_cache_delete VALUES (1, 100, 'A', 1, 'stale');
SELECT payload FROM overwrite_cache_delete WHERE website_type = 1 AND user_id = 100 AND tag = 'A';
INSERT INTO overwrite_cache_delete VALUES (1, 100, 'A', 2, 'a-one-newer');
SELECT payload FROM overwrite_cache_delete WHERE website_type = 1 AND user_id = 100 AND tag = 'A';

SELECT '-- the scalar lookup functions see the deletion';
DELETE FROM overwrite_cache_delete WHERE website_type = 2 AND user_id = 400 AND tag = 'C';
SELECT overwriteCacheGetOrNull(concat(currentDatabase(), '.overwrite_cache_delete'), 'payload', toUInt8(2), toUInt64(400), 'C') IS NULL;

SELECT '-- ALTER TABLE ... DELETE WHERE works the same way';
ALTER TABLE overwrite_cache_delete DELETE WHERE website_type = 1 AND user_id = 100 AND tag = 'A';
SELECT payload FROM overwrite_cache_delete WHERE tag = 'A';
SELECT total_rows FROM system.tables WHERE database = currentDatabase() AND name = 'overwrite_cache_delete';

SELECT '-- other mutations stay unsupported';
ALTER TABLE overwrite_cache_delete UPDATE payload = 'x' WHERE website_type = 1; -- { serverError NOT_IMPLEMENTED }

DROP TABLE overwrite_cache_delete;

SELECT '-- a failed publication leaves every row in place';

CREATE TABLE overwrite_cache_delete_rollback
(
    key UInt64,
    tag String,
    version UInt64,
    payload String
)
ENGINE = OverwriteCache(version)
KEYS (key, tag)
INDEX (tag);

INSERT INTO overwrite_cache_delete_rollback VALUES (1, 'a', 1, 'one'), (2, 'a', 1, 'two');

SYSTEM ENABLE FAILPOINT overwrite_cache_throw_during_publish;
DELETE FROM overwrite_cache_delete_rollback WHERE key = 1 AND tag = 'a'; -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT overwrite_cache_throw_during_publish;

SELECT payload FROM overwrite_cache_delete_rollback WHERE key = 1 AND tag = 'a';
SELECT payload FROM overwrite_cache_delete_rollback WHERE tag = 'a' ORDER BY payload;
SELECT total_rows FROM system.tables WHERE database = currentDatabase() AND name = 'overwrite_cache_delete_rollback';

DELETE FROM overwrite_cache_delete_rollback WHERE key = 1 AND tag = 'a';
SELECT payload FROM overwrite_cache_delete_rollback WHERE tag = 'a' ORDER BY payload;
SELECT total_rows FROM system.tables WHERE database = currentDatabase() AND name = 'overwrite_cache_delete_rollback';

DROP TABLE overwrite_cache_delete_rollback;

DROP TABLE IF EXISTS overwrite_cache_delete_bytes;
DROP TABLE IF EXISTS overwrite_cache_delete_probe;

CREATE TABLE overwrite_cache_delete_bytes
(
    key UInt64,
    version UInt64,
    payload String
)
ENGINE = OverwriteCache(version)
KEYS (key);

CREATE TABLE overwrite_cache_delete_probe (stage String, bytes UInt64) ENGINE = Memory;

INSERT INTO overwrite_cache_delete_bytes SELECT number, 1, repeat('x', 1000) FROM numbers(64);
SELECT total_rows FROM system.tables WHERE database = currentDatabase() AND name = 'overwrite_cache_delete_bytes';
INSERT INTO overwrite_cache_delete_probe
SELECT 'filled', total_bytes FROM system.tables WHERE database = currentDatabase() AND name = 'overwrite_cache_delete_bytes';

SELECT '-- an IN list and an IN subquery both resolve keys';
DELETE FROM overwrite_cache_delete_bytes WHERE key IN (0, 1, 2, 3);
SELECT total_rows FROM system.tables WHERE database = currentDatabase() AND name = 'overwrite_cache_delete_bytes';
DELETE FROM overwrite_cache_delete_bytes WHERE key IN (SELECT number FROM numbers(64));
SELECT total_rows FROM system.tables WHERE database = currentDatabase() AND name = 'overwrite_cache_delete_bytes';
INSERT INTO overwrite_cache_delete_probe
SELECT 'emptied', total_bytes FROM system.tables WHERE database = currentDatabase() AND name = 'overwrite_cache_delete_bytes';

SELECT '-- a segment left at least half dead is compacted';
INSERT INTO overwrite_cache_delete_bytes SELECT number, 1, repeat('y', 1000) FROM numbers(64);
DELETE FROM overwrite_cache_delete_bytes WHERE key IN (SELECT number FROM numbers(48));
SELECT total_rows FROM system.tables WHERE database = currentDatabase() AND name = 'overwrite_cache_delete_bytes';
INSERT INTO overwrite_cache_delete_probe
SELECT 'compacted', total_bytes FROM system.tables WHERE database = currentDatabase() AND name = 'overwrite_cache_delete_bytes';

-- The primary index and the entry table never shrink, so the fully deleted table is the baseline the
-- payload is measured against. Deleting three quarters of a segment must leave about a quarter of it.
SELECT
    (SELECT bytes FROM overwrite_cache_delete_probe WHERE stage = 'filled')
        - (SELECT bytes FROM overwrite_cache_delete_probe WHERE stage = 'emptied') > 60000,
    (SELECT bytes FROM overwrite_cache_delete_probe WHERE stage = 'compacted')
        - (SELECT bytes FROM overwrite_cache_delete_probe WHERE stage = 'emptied') BETWEEN 1 AND 30000;

DROP TABLE overwrite_cache_delete_probe;
DROP TABLE overwrite_cache_delete_bytes;
