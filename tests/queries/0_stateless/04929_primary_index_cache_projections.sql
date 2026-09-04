-- Projection parts must not keep their primary indexes pinned in memory either,
-- including a mutation that rebuilds only the projection (the main part's index
-- is hardlinked and stays absent).

DROP TABLE IF EXISTS t_pk_cache_proj;

CREATE TABLE t_pk_cache_proj (a UInt64, b UInt64, c UInt64, PROJECTION p (SELECT b, a ORDER BY b))
ENGINE = MergeTree ORDER BY a
SETTINGS use_primary_key_cache = 1, prewarm_primary_key_cache = 1, index_granularity = 64;

-- { echoOn }
INSERT INTO t_pk_cache_proj SELECT number, number, number FROM numbers(10000);
SELECT sum(primary_key_bytes_in_memory) FROM system.parts WHERE database = currentDatabase() AND table = 't_pk_cache_proj' AND active;
SELECT sum(primary_key_bytes_in_memory) FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_pk_cache_proj' AND active;
ALTER TABLE t_pk_cache_proj UPDATE b = b + 1 WHERE 1 SETTINGS mutations_sync = 1;
SELECT count() FROM t_pk_cache_proj WHERE b > 100;
SELECT sum(primary_key_bytes_in_memory) FROM system.parts WHERE database = currentDatabase() AND table = 't_pk_cache_proj' AND active;
SELECT sum(primary_key_bytes_in_memory) FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_pk_cache_proj' AND active;
INSERT INTO t_pk_cache_proj SELECT number, number, number FROM numbers(10000);
OPTIMIZE TABLE t_pk_cache_proj FINAL;
SELECT sum(primary_key_bytes_in_memory) FROM system.parts WHERE database = currentDatabase() AND table = 't_pk_cache_proj' AND active;
SELECT sum(primary_key_bytes_in_memory) FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_pk_cache_proj' AND active;
-- { echoOff }

DROP TABLE t_pk_cache_proj;
