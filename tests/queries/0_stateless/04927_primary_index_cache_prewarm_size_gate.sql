-- A part that shrinks below `min_bytes_to_prewarm_caches` between the write-time
-- estimate and the actual committed size (rows deleted by a mutation, duplicates
-- collapsed by a merge) must not keep its primary index pinned in memory.

DROP TABLE IF EXISTS t_pk_cache_size_gate;
DROP TABLE IF EXISTS t_pk_cache_size_gate_merge;

CREATE TABLE t_pk_cache_size_gate (a UInt64, b UInt64, c UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS use_primary_key_cache = 1, prewarm_primary_key_cache = 1, min_bytes_to_prewarm_caches = 1000000;

CREATE TABLE t_pk_cache_size_gate_merge (a UInt64, b UInt64, c UInt64)
ENGINE = ReplacingMergeTree ORDER BY a
SETTINGS use_primary_key_cache = 1, prewarm_primary_key_cache = 1, min_bytes_to_prewarm_caches = 1000000;

SYSTEM STOP MERGES t_pk_cache_size_gate_merge;

-- { echoOn }
INSERT INTO t_pk_cache_size_gate SELECT number, number, number FROM numbers(100000);
SELECT sum(primary_key_bytes_in_memory) FROM system.parts WHERE database = currentDatabase() AND table = 't_pk_cache_size_gate' AND active;
ALTER TABLE t_pk_cache_size_gate DELETE WHERE a >= 10000 SETTINGS mutations_sync = 1;
SELECT count() FROM t_pk_cache_size_gate;
SELECT sum(primary_key_bytes_in_memory) FROM system.parts WHERE database = currentDatabase() AND table = 't_pk_cache_size_gate' AND active;
INSERT INTO t_pk_cache_size_gate_merge SELECT number, number, number FROM numbers(30000);
INSERT INTO t_pk_cache_size_gate_merge SELECT number, number, number FROM numbers(30000);
SYSTEM START MERGES t_pk_cache_size_gate_merge;
OPTIMIZE TABLE t_pk_cache_size_gate_merge FINAL;
SELECT count() FROM t_pk_cache_size_gate_merge;
SELECT sum(primary_key_bytes_in_memory) FROM system.parts WHERE database = currentDatabase() AND table = 't_pk_cache_size_gate_merge' AND active;
-- { echoOff }

DROP TABLE t_pk_cache_size_gate;
DROP TABLE t_pk_cache_size_gate_merge;
