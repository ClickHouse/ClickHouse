-- `getPostingIds` merges one sorted run per distinct key of an `IN (...)` list. When every posting holds
-- only the one row its key ever received, a large list divides the collected identifiers into as many
-- one-element runs as there are keys, which must fall back to sorting the whole collection once instead
-- of merging thousands of one-element runs pairwise. Either way the result must be the same.

DROP TABLE IF EXISTS overwrite_cache_singleton_postings;

CREATE TABLE overwrite_cache_singleton_postings
(
    id UInt64,
    shard UInt64,
    version UInt64,
    payload String
)
ENGINE = OverwriteCache(version)
KEYS (id, shard)
INDEX (shard);

-- Every `shard` value belongs to exactly one row.
INSERT INTO overwrite_cache_singleton_postings SELECT number, number, 1, concat('v', toString(number)) FROM numbers(5000);

SELECT '-- every key resolves through the singleton-posting path';
SELECT count(), uniqExact(payload) FROM overwrite_cache_singleton_postings WHERE shard IN (SELECT number FROM numbers(5000));

SELECT '-- a repeated key in the list is not double-counted';
SELECT count() FROM overwrite_cache_singleton_postings WHERE shard IN (SELECT number % 2500 FROM numbers(5000));

SELECT '-- a list mixing present and absent keys keeps only the matches';
SELECT count() FROM overwrite_cache_singleton_postings WHERE shard IN (SELECT number FROM numbers(2500, 5000));

SELECT '-- deleting half the keys leaves the other half reachable the same way';
DELETE FROM overwrite_cache_singleton_postings WHERE shard IN (SELECT number * 2 FROM numbers(2500));
SELECT count() FROM overwrite_cache_singleton_postings WHERE shard IN (SELECT number FROM numbers(5000));

DROP TABLE overwrite_cache_singleton_postings;
