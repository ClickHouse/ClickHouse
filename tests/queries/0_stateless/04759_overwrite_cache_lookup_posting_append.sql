-- Publication allocates entry identifiers above every identifier a lookup posting already holds, so it
-- appends to the tail. Only a resurrected key, which reuses the identifier it had before its tombstone,
-- can add an identifier below the tail and needs the general merge. Both paths must leave the posting
-- sorted and free of duplicates, because `intersectPostingIds` binary-searches it.

DROP TABLE IF EXISTS overwrite_cache_posting_append;

CREATE TABLE overwrite_cache_posting_append
(
    website_type UInt8,
    user_id UInt64,
    tag LowCardinality(String),
    version UInt64,
    payload String
)
ENGINE = OverwriteCache(version)
KEYS (website_type, user_id, tag)
INDEX (website_type, tag), (website_type, user_id), (tag);

SELECT '-- successive batches append to one shared posting';
INSERT INTO overwrite_cache_posting_append SELECT 1, number, 'hot', 1, 'p1' FROM numbers(1000);
INSERT INTO overwrite_cache_posting_append SELECT 1, number, 'hot', 1, 'p2' FROM numbers(1000, 1000);
INSERT INTO overwrite_cache_posting_append SELECT 1, number, 'hot', 1, 'p3' FROM numbers(2000, 1000);
SELECT count(), uniqExact(user_id), min(user_id), max(user_id) FROM overwrite_cache_posting_append WHERE tag = 'hot';
SELECT count() FROM overwrite_cache_posting_append WHERE website_type = 1 AND tag = 'hot';

SELECT '-- a second key value interleaves without disturbing the first';
INSERT INTO overwrite_cache_posting_append SELECT 1, number, 'cold', 1, 'c1' FROM numbers(500);
SELECT count() FROM overwrite_cache_posting_append WHERE tag = 'hot';
SELECT count() FROM overwrite_cache_posting_append WHERE tag = 'cold';
SELECT count() FROM overwrite_cache_posting_append WHERE website_type = 1 AND tag = 'cold';

SELECT '-- overwriting live keys adds no posting entries';
INSERT INTO overwrite_cache_posting_append SELECT 1, number, 'hot', 2, 'p-newer' FROM numbers(3000);
SELECT count(), uniqExact(payload) FROM overwrite_cache_posting_append WHERE tag = 'hot';
SELECT total_rows FROM system.tables WHERE database = currentDatabase() AND name = 'overwrite_cache_posting_append';

SELECT '-- one batch mixes a resurrected identifier with identifiers past the tail';
DELETE FROM overwrite_cache_posting_append WHERE website_type = 1 AND user_id = 7 AND tag = 'hot';
SELECT count() FROM overwrite_cache_posting_append WHERE tag = 'hot';
INSERT INTO overwrite_cache_posting_append VALUES
    (1, 7, 'hot', 3, 'resurrected'),
    (1, 9001, 'hot', 1, 'fresh-a'),
    (1, 9002, 'hot', 1, 'fresh-b');
SELECT count() FROM overwrite_cache_posting_append WHERE tag = 'hot';
SELECT count() FROM overwrite_cache_posting_append WHERE website_type = 1 AND tag = 'hot';
SELECT payload FROM overwrite_cache_posting_append WHERE website_type = 1 AND user_id = 7 AND tag = 'hot';
SELECT user_id, payload FROM overwrite_cache_posting_append
WHERE tag = 'hot' AND payload != 'p-newer' ORDER BY user_id;

DROP TABLE overwrite_cache_posting_append;

SELECT '-- a merged posting stays binary-searchable when two indexes are intersected';

DROP TABLE IF EXISTS overwrite_cache_posting_intersect;

CREATE TABLE overwrite_cache_posting_intersect
(
    shard UInt8,
    tag String,
    bucket UInt8,
    user_id UInt64,
    version UInt64,
    payload String
)
ENGINE = OverwriteCache(version)
KEYS (shard, tag, bucket, user_id)
INDEX (shard, tag), (bucket);

INSERT INTO overwrite_cache_posting_intersect SELECT 1, 'hot', number % 4, number, 1, 'v1' FROM numbers(400);
SELECT count() FROM overwrite_cache_posting_intersect WHERE shard = 1 AND tag = 'hot' AND bucket = 0;

DELETE FROM overwrite_cache_posting_intersect WHERE shard = 1 AND tag = 'hot' AND bucket = 0 AND user_id = 4;
SELECT count() FROM overwrite_cache_posting_intersect WHERE shard = 1 AND tag = 'hot' AND bucket = 0;

-- The tombstone is pruned by the next writer, which is this insert, so the resurrected identifier is
-- absent from both postings when the batch is prepared and re-enters them below the tail.
INSERT INTO overwrite_cache_posting_intersect VALUES
    (1, 'hot', 0, 4, 2, 'resurrected'),
    (1, 'hot', 0, 5000, 1, 'fresh-a'),
    (1, 'hot', 0, 5001, 1, 'fresh-b');

SELECT count() FROM overwrite_cache_posting_intersect WHERE shard = 1 AND tag = 'hot' AND bucket = 0;
SELECT user_id, payload FROM overwrite_cache_posting_intersect
WHERE shard = 1 AND tag = 'hot' AND bucket = 0 AND payload != 'v1' ORDER BY user_id;
SELECT count() FROM overwrite_cache_posting_intersect WHERE bucket = 0;
SELECT count() FROM overwrite_cache_posting_intersect WHERE shard = 1 AND tag = 'hot';

DROP TABLE overwrite_cache_posting_intersect;
