-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database
-- Lightweight (on-the-fly) mutations combined with a warmed columns cache.
-- A cache hit for the base part columns must still flow through the on-the-fly
-- mutation overlays (`ALTER ... UPDATE` / `ALTER ... DELETE`) and return the same
-- rows as a cold disk read. This is the realistic default combination once the
-- columns cache is enabled by default, so it must stay covered.

SET apply_mutations_on_fly = 1;
SET use_columns_cache = 1;
SET enable_reads_from_columns_cache = 1;
SET enable_writes_to_columns_cache = 1;
SET max_threads = 1; -- deterministic read order for the cache

SYSTEM DROP COLUMNS CACHE;
DROP TABLE IF EXISTS t_cache_on_fly;

CREATE TABLE t_cache_on_fly (id UInt64, v UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

-- Stop merges so the mutations below stay pending and are applied on the fly.
SYSTEM STOP MERGES t_cache_on_fly;

INSERT INTO t_cache_on_fly SELECT number, number, toString(number) FROM numbers(3000);

-- Warm the cache with the base columns (second read is a cache hit).
SELECT sum(v), sum(length(s)), count() FROM t_cache_on_fly;
SELECT sum(v), sum(length(s)), count() FROM t_cache_on_fly;

-- The cache now holds entries for the base part.
SELECT count() > 0 FROM system.columns_cache
WHERE database = currentDatabase() AND table = 't_cache_on_fly';

-- Lightweight UPDATE applied on the fly (merges are stopped, so it is not materialized).
ALTER TABLE t_cache_on_fly UPDATE v = v + 1000000 WHERE id < 1000;

-- A read served from the warmed cache, with the update overlay applied, must match a cold disk read.
SELECT sum(v), sum(length(s)), count() FROM t_cache_on_fly SETTINGS use_columns_cache = 1;
SELECT sum(v), sum(length(s)), count() FROM t_cache_on_fly SETTINGS use_columns_cache = 0;

-- Lightweight DELETE applied on the fly.
ALTER TABLE t_cache_on_fly DELETE WHERE id >= 2000;

SELECT sum(v), sum(length(s)), count() FROM t_cache_on_fly SETTINGS use_columns_cache = 1;
SELECT sum(v), sum(length(s)), count() FROM t_cache_on_fly SETTINGS use_columns_cache = 0;

DROP TABLE t_cache_on_fly;
