-- Tags: zookeeper

DROP TABLE IF EXISTS t_pk_cache_mutation;
DROP TABLE IF EXISTS t_pk_cache_mutation_rmt;

-- A mutation rewriting the whole part must leave the new part's primary index
-- in the cache (evictable), not pinned in memory.

CREATE TABLE t_pk_cache_mutation (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS use_primary_key_cache = 1, prewarm_primary_key_cache = 1, index_granularity = 64;

CREATE TABLE t_pk_cache_mutation_rmt (a UInt64, b UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_pk_cache_mutation_rmt', '1') ORDER BY a
SETTINGS use_primary_key_cache = 1, prewarm_primary_key_cache = 1, index_granularity = 64;

-- { echoOn }
INSERT INTO t_pk_cache_mutation SELECT number, number FROM numbers(10000);
SELECT sum(primary_key_bytes_in_memory) FROM system.parts WHERE database = currentDatabase() AND table = 't_pk_cache_mutation' AND active;
ALTER TABLE t_pk_cache_mutation DELETE WHERE a % 2 = 0 SETTINGS mutations_sync = 1;
SELECT count() FROM t_pk_cache_mutation WHERE a > 100 AND a < 1000;
SELECT sum(primary_key_bytes_in_memory) FROM system.parts WHERE database = currentDatabase() AND table = 't_pk_cache_mutation' AND active;
INSERT INTO t_pk_cache_mutation_rmt SELECT number, number FROM numbers(10000);
SELECT sum(primary_key_bytes_in_memory) FROM system.parts WHERE database = currentDatabase() AND table = 't_pk_cache_mutation_rmt' AND active;
ALTER TABLE t_pk_cache_mutation_rmt DELETE WHERE a % 2 = 0 SETTINGS mutations_sync = 2;
SELECT count() FROM t_pk_cache_mutation_rmt WHERE a > 100 AND a < 1000;
SELECT sum(primary_key_bytes_in_memory) FROM system.parts WHERE database = currentDatabase() AND table = 't_pk_cache_mutation_rmt' AND active;
-- { echoOff }

DROP TABLE t_pk_cache_mutation;
DROP TABLE t_pk_cache_mutation_rmt;
