-- Tags: no-parallel, no-parallel-replicas, no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage, no-async-insert
-- Tag no-parallel: drops the (instance-wide) query condition cache
-- no-parallel-replicas: the cache is populated per replica, so the mark counts below are
--                       deterministic only on a single replica
-- no-fasttest: a UNIQUE KEY insert writes the dense-index SST, which needs RocksDB
-- no-object-storage, no-s3-storage: UNIQUE KEY requires a storage policy of local disks
-- no-ordinary-database, no-replicated-database, no-shared-merge-tree: UNIQUE KEY is only supported
--                       on plain MergeTree in an Atomic database

-- Why the query condition cache and UNIQUE KEY tables do not interact, pinned so that changing
-- either half is deliberate.
--
-- 1. A UNIQUE KEY read never uses the cache. `ReadFromMergeTree` turns it off for both the write and
--    the consult side, because the cache is CSN-oblivious while the delete bitmap is not: a mark
--    recorded as non-matching after a bitmap drop could be skipped by a reader pinned at an older
--    snapshot whose rows are still live.
-- 2. No UNIQUE KEY part can carry a materialized `_row_exists` mask, because mutation-class commands
--    are rejected on such tables - `DELETE FROM` included, as it is executed as an
--    `UPDATE _row_exists`.
--
-- Either one on its own is enough to keep such tables away from the materialized-mask handling in
-- `appliesMutationsBeforePrewhere`. Re-enabling the cache for UNIQUE KEY reads (there is a TODO for
-- a snapshot-aware cache) makes this test fail, which is the point: that work has to look at the
-- mask and bitmap interaction rather than just flipping the flag.

SET allow_experimental_unique_key = 1;
SET async_insert = 0;
SET use_query_condition_cache = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_qcc_uk;

CREATE TABLE t_qcc_uk (id UInt64, v UInt64)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, auto_statistics_types = '';

INSERT INTO t_qcc_uk SELECT number, number FROM numbers(100000);

SELECT '--- a UNIQUE KEY part cannot carry a materialized _row_exists mask';
DELETE FROM t_qcc_uk WHERE id = 0; -- { serverError SUPPORT_IS_DISABLED }

SELECT sum(has_lightweight_delete) FROM system.parts
WHERE database = currentDatabase() AND table = 't_qcc_uk' AND active;

SELECT '--- and a UNIQUE KEY read does not use the cache at all';
SYSTEM DROP QUERY CONDITION CACHE;

-- `v = 123456789` matches no row. On a plain MergeTree table the second run would hit the cache and
-- read no marks (see 04669_query_condition_cache_lightweight_delete); here neither run may.
SELECT count() FROM t_qcc_uk WHERE v = 123456789 SETTINGS log_comment = '04670_uk_prime';
SELECT count() FROM t_qcc_uk WHERE v = 123456789 SETTINGS log_comment = '04670_uk_reuse';

SYSTEM FLUSH LOGS query_log;

-- Columns: (any QCC hit), (read no marks at all). Expected: both runs = 0 0.
SELECT
    log_comment,
    ProfileEvents['QueryConditionCacheHits'] > 0,
    ProfileEvents['SelectedMarks'] = 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment IN ('04670_uk_prime', '04670_uk_reuse')
ORDER BY event_time_microseconds;

SELECT '--- results are correct';
SELECT count() FROM t_qcc_uk;
SELECT count() FROM t_qcc_uk WHERE v < 10;

DROP TABLE t_qcc_uk;
