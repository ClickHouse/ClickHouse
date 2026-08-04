-- Tags: no-parallel, no-parallel-replicas, no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage, no-async-insert
-- no-parallel: drops the (instance-wide) query condition cache
-- no-parallel-replicas: the cache is populated per replica, so the mark counts below are
--                       deterministic only on a single replica
-- no-fasttest: a UNIQUE KEY insert writes the dense-index SST, which needs RocksDB
-- no-object-storage, no-s3-storage: UNIQUE KEY requires a storage policy of local disks
-- no-ordinary-database, no-replicated-database, no-shared-merge-tree: UNIQUE KEY is only supported
--                       on plain MergeTree in an Atomic database

-- UNIQUE KEY tables and the query condition cache.
--
-- Building a part's dense index reads the part's UNIQUE KEY columns with `apply_deleted_mask = 0`
-- (`UniqueKeyDenseIndexOps::readUniqueKeyColumns`), which is the one internal reader that turns the
-- mask off. That read carries no predicate, so it neither writes nor consults the cache, and user
-- queries on the same table must keep pruning normally.

SET allow_experimental_unique_key = 1;
SET async_insert = 0;
SET use_query_condition_cache = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_qcc_uk;

-- auto_statistics_types = '': randomized auto statistics would prune the whole part for the
-- never-matching predicate below, leaving nothing to read and the mark counts vacuous.
CREATE TABLE t_qcc_uk (id UInt64, v UInt64)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, auto_statistics_types = '';

INSERT INTO t_qcc_uk SELECT number, number FROM numbers(100000);

SELECT '--- a UNIQUE KEY part cannot carry a materialized _row_exists mask';
-- Mutation-class commands are rejected on a UNIQUE KEY table, `DELETE FROM` included (it is
-- executed as an `UPDATE _row_exists`), so the materialized-mask case cannot arise here at all.
DELETE FROM t_qcc_uk WHERE id = 0; -- { serverError SUPPORT_IS_DISABLED }

SELECT sum(has_lightweight_delete) FROM system.parts
WHERE database = currentDatabase() AND table = 't_qcc_uk' AND active;

SELECT '--- the cache still prunes for ordinary queries';
SYSTEM DROP QUERY CONDITION CACHE;

SELECT count() FROM t_qcc_uk WHERE v = 123456789 SETTINGS log_comment = '04670_uk_prime';
SELECT count() FROM t_qcc_uk WHERE v = 123456789 SETTINGS log_comment = '04670_uk_reuse';

SYSTEM FLUSH LOGS query_log;

-- Columns: (any QCC hit), (read no marks at all). Expected: prime = 0 0, reuse = 1 1.
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

SELECT '--- results stay correct with the cache warm';
SELECT count() FROM t_qcc_uk;
SELECT count() FROM t_qcc_uk WHERE v < 10;

DROP TABLE t_qcc_uk;
