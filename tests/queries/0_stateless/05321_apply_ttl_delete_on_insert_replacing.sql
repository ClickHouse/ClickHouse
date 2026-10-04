-- For the `MergeTree` engines which merge rows, `apply_ttl_delete_on_insert` applies `TTL ... DELETE`
-- to the rows produced by merging the inserted block, as a TTL merge does.

DROP TABLE IF EXISTS t_ttl_insert_replacing;
DROP TABLE IF EXISTS t_ttl_insert_summing;

CREATE TABLE t_ttl_insert_replacing (key UInt64, version UInt64, ts DateTime)
ENGINE = ReplacingMergeTree(version) ORDER BY key TTL ts + INTERVAL 1 DAY DELETE;

-- The newest version of key 1 is expired, so it replaces the older version and is removed: no row remains.
-- The newest version of key 2 is not expired, and it is kept.
INSERT INTO t_ttl_insert_replacing SETTINGS apply_ttl_delete_on_insert = 1, optimize_on_insert = 1 VALUES (1, 1, now()), (1, 2, now() - INTERVAL 2 DAY), (2, 1, now() - INTERVAL 2 DAY), (2, 2, now());
SELECT 'replacing', key, version FROM t_ttl_insert_replacing ORDER BY key;
TRUNCATE TABLE t_ttl_insert_replacing;

-- Without merging on insert the TTL is not applied: the expired rows are kept until a TTL merge.
SYSTEM STOP MERGES t_ttl_insert_replacing;
INSERT INTO t_ttl_insert_replacing SETTINGS apply_ttl_delete_on_insert = 1, optimize_on_insert = 0 VALUES (1, 1, now()), (1, 2, now() - INTERVAL 2 DAY);
SELECT 'not merged', key, version FROM t_ttl_insert_replacing ORDER BY key, version;

CREATE TABLE t_ttl_insert_summing (key UInt64, ts DateTime, value UInt64)
ENGINE = SummingMergeTree(value) ORDER BY key TTL ts + INTERVAL 1 DAY DELETE;

-- The rows are summed first, the merged row keeps `ts` of the first row of the key.
INSERT INTO t_ttl_insert_summing SETTINGS apply_ttl_delete_on_insert = 1, optimize_on_insert = 1 VALUES (1, now(), 1), (1, now() - INTERVAL 2 DAY, 10), (2, now() - INTERVAL 2 DAY, 1), (3, now() - INTERVAL 2 DAY, 1), (3, now(), 10);
SELECT 'summing', key, value FROM t_ttl_insert_summing ORDER BY key;

DROP TABLE t_ttl_insert_replacing;
DROP TABLE t_ttl_insert_summing;
