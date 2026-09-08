-- Wrapping or unwrapping LowCardinality on a key column is allowed and does not rewrite existing
-- parts, so until the mutation materializes a part's primary index holds a different representation
-- than the current metadata declares. mergeTreeIndex must still emit the declared type.

DROP TABLE IF EXISTS t_mt_index_key_type;

SELECT '-- String -> LowCardinality(String)';

-- use_primary_key_cache = 0 keeps the stale index in the part itself; a cache entry is evictable,
-- and a reload rebuilds the column with the current type, which would make the reads below vacuous.
CREATE TABLE t_mt_index_key_type (k String, v UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 2, index_granularity_bytes = '10M', use_primary_key_cache = 0;

SYSTEM STOP MERGES t_mt_index_key_type;
INSERT INTO t_mt_index_key_type SELECT toString(number), number FROM numbers(6);

SELECT k FROM mergeTreeIndex(currentDatabase(), t_mt_index_key_type) ORDER BY k;

ALTER TABLE t_mt_index_key_type MODIFY COLUMN k LowCardinality(String)
    SETTINGS alter_sync = 0, mutations_sync = 0;

-- The rewriting mutation must still be pending, otherwise the reads below cover nothing.
SELECT count() > 0 FROM system.mutations
WHERE database = currentDatabase() AND table = 't_mt_index_key_type' AND NOT is_done;

SELECT k FROM mergeTreeIndex(currentDatabase(), t_mt_index_key_type) ORDER BY k;

-- A part written after the ALTER already carries the new representation: one read, both parts.
INSERT INTO t_mt_index_key_type SELECT toString(number), number FROM numbers(6, 4);
SELECT k FROM mergeTreeIndex(currentDatabase(), t_mt_index_key_type) ORDER BY k;

DROP TABLE t_mt_index_key_type;

SELECT '-- LowCardinality(String) -> String';

CREATE TABLE t_mt_index_key_type (k LowCardinality(String), v UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 2, index_granularity_bytes = '10M', use_primary_key_cache = 0;

SYSTEM STOP MERGES t_mt_index_key_type;
INSERT INTO t_mt_index_key_type SELECT toString(number), number FROM numbers(6);

SELECT k FROM mergeTreeIndex(currentDatabase(), t_mt_index_key_type) ORDER BY k;

ALTER TABLE t_mt_index_key_type MODIFY COLUMN k String
    SETTINGS alter_sync = 0, mutations_sync = 0;

SELECT count() > 0 FROM system.mutations
WHERE database = currentDatabase() AND table = 't_mt_index_key_type' AND NOT is_done;

SELECT k FROM mergeTreeIndex(currentDatabase(), t_mt_index_key_type) ORDER BY k;

DROP TABLE t_mt_index_key_type;
