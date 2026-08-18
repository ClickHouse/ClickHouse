DROP TABLE IF EXISTS t_pk_unload_on_enable;

-- Enabling the cache must unload indexes loaded in parts while it was disabled.

CREATE TABLE t_pk_unload_on_enable (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS use_primary_key_cache = 0, index_granularity = 64;

-- { echoOn }
INSERT INTO t_pk_unload_on_enable SELECT number, number FROM numbers(10000);
SELECT sum(primary_key_bytes_in_memory) > 0 FROM system.parts WHERE database = currentDatabase() AND table = 't_pk_unload_on_enable' AND active;
ALTER TABLE t_pk_unload_on_enable MODIFY SETTING use_primary_key_cache = 1;
SELECT sum(primary_key_bytes_in_memory) FROM system.parts WHERE database = currentDatabase() AND table = 't_pk_unload_on_enable' AND active;
SELECT count() FROM t_pk_unload_on_enable WHERE a > 100 AND a < 1000;
SELECT sum(primary_key_bytes_in_memory) FROM system.parts WHERE database = currentDatabase() AND table = 't_pk_unload_on_enable' AND active;
-- { echoOff }

DROP TABLE t_pk_unload_on_enable;
