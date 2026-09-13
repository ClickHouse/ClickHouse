-- Tags: no-random-merge-tree-settings
-- ^ The TTL GROUP BY merge must actually run; pin MergeTree settings so the inputs are
--   reliably merged into a single part.

-- The re-sort a merge performs after `TTL ... GROUP BY ... SET` on a sorting key column must
-- not buffer the whole part in memory: background merge and mutation contexts disable the
-- external sort by default (`max_bytes_before_external_sort = 0`), so the sort is bounded by
-- the `ttl_resort_max_bytes_before_external_sort` MergeTree setting instead. With the
-- threshold forced to 1 byte, every accumulated block must be spilled to disk and the merge
-- must still produce a correct, physically sorted part.

DROP TABLE IF EXISTS t_ttl_resort_spill;
CREATE TABLE t_ttl_resort_spill (k Float64, ts DateTime, v Float64)
ENGINE = MergeTree ORDER BY (k, toStartOfDay(ts))
TTL ts + toIntervalDay(1) GROUP BY k, toStartOfDay(ts)
    SET ts = max(ts) + interval 100 years, k = max(v)
SETTINGS min_bytes_for_full_part_storage = 128, ttl_resort_max_bytes_before_external_sort = 1;

SYSTEM STOP MERGES t_ttl_resort_spill;
-- Expired rows: 10 keys x 3 days = 30 groups, each SET to a non-monotonic key (max(v)).
INSERT INTO t_ttl_resort_spill
    SELECT number % 10, toDateTime('2000-06-09 10:00:00') + (number % 3) * 86400, 100000 - number
    FROM numbers(50000);
-- Non-expired rows pass through the TTL step unchanged and take part in the re-sort too.
INSERT INTO t_ttl_resort_spill
    SELECT 1000000 + number % 100, toDateTime('2106-01-01 00:00:00') + number % 1000, number
    FROM numbers(50000);
SYSTEM START MERGES t_ttl_resort_spill;
OPTIMIZE TABLE t_ttl_resort_spill FINAL;

SELECT 'count', count() FROM t_ttl_resort_spill;
-- The aggregated groups must survive the spill: 30 groups, keys rewritten to max(v).
SELECT 'aggregated', count(), min(k), max(k) FROM t_ttl_resort_spill WHERE k < 1000000;
-- Part must be physically sorted: natural read order equals ORDER BY read order.
SELECT 'sorted', (SELECT groupArray((k, toStartOfDay(ts))) FROM (SELECT k, ts FROM t_ttl_resort_spill SETTINGS optimize_read_in_order = 0))
               = (SELECT groupArray((k, toStartOfDay(ts))) FROM (SELECT k, ts FROM t_ttl_resort_spill ORDER BY k, toStartOfDay(ts)));

-- The sort must actually have spilled: the TTL merge writes external-sort temporary files.
-- Aggregate over all merges: a follow-up merge of the already-aggregated part has nothing
-- expired, skips the TTL step entirely and correctly does not sort or spill.
SYSTEM FLUSH LOGS part_log;
SELECT 'spilled', max(ProfileEvents['ExternalSortWritePart']) > 0
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_ttl_resort_spill' AND event_type = 'MergeParts' AND error = 0;

DROP TABLE t_ttl_resort_spill;

-- The mutation path (`ALTER TABLE ... MATERIALIZE TTL`) uses the same bounded sort.
DROP TABLE IF EXISTS t_ttl_resort_spill_mat;
CREATE TABLE t_ttl_resort_spill_mat (k Float64, ts DateTime, v Float64)
ENGINE = MergeTree ORDER BY (k, toStartOfDay(ts))
SETTINGS min_bytes_for_full_part_storage = 128, ttl_resort_max_bytes_before_external_sort = 1,
         materialize_ttl_recalculate_only = 0;

-- Stop TTL merges so the TTL is applied by the MATERIALIZE TTL mutation below, not by a
-- background TTL merge racing it.
SYSTEM STOP TTL MERGES t_ttl_resort_spill_mat;

INSERT INTO t_ttl_resort_spill_mat
    SELECT number % 10, toDateTime('2000-06-09 10:00:00') + (number % 3) * 86400, 100000 - number
    FROM numbers(50000);

ALTER TABLE t_ttl_resort_spill_mat
    MODIFY TTL ts + toIntervalDay(1) GROUP BY k, toStartOfDay(ts)
        SET ts = max(ts) + interval 100 years, k = max(v)
    SETTINGS mutations_sync = 2;
ALTER TABLE t_ttl_resort_spill_mat MATERIALIZE TTL SETTINGS mutations_sync = 2;

SELECT 'mat count', count() FROM t_ttl_resort_spill_mat;
SELECT 'mat sorted', (SELECT groupArray((k, toStartOfDay(ts))) FROM (SELECT k, ts FROM t_ttl_resort_spill_mat SETTINGS optimize_read_in_order = 0))
                   = (SELECT groupArray((k, toStartOfDay(ts))) FROM (SELECT k, ts FROM t_ttl_resort_spill_mat ORDER BY k, toStartOfDay(ts)));

SYSTEM FLUSH LOGS part_log;
SELECT 'mat spilled', max(ProfileEvents['ExternalSortWritePart']) > 0
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_ttl_resort_spill_mat' AND event_type = 'MutatePart' AND error = 0;

DROP TABLE t_ttl_resort_spill_mat;
