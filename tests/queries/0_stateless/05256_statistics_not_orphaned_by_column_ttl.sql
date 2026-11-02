-- Tests that a column TTL which empties a column does not leave a stale statistic for it behind:
-- min()/max() and a filtered count() on that column must agree with the rows the table stores.

SET mutations_sync = 2, alter_sync = 2, materialize_statistics_on_insert = 1;
-- Pin every gate the two statistics consumers below depend on, so that neither a randomized draw
-- nor a future default flip can disarm this test silently.
SET use_statistics = 1, use_statistics_for_part_pruning = 1, use_statistics_for_min_max_aggregation = 1;
SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SET optimize_aggregation_in_order = 0, force_aggregation_in_order = 0, aggregate_functions_null_for_empty = 0;
SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET automatic_parallel_replicas_mode = 0, enable_parallel_replicas = 0;

DROP TABLE IF EXISTS ttl_stats_orphan;
CREATE TABLE ttl_stats_orphan (d DateTime, v UInt32 DEFAULT 7 STATISTICS(basic) TTL d + INTERVAL 1 SECOND)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, auto_statistics_types = 'basic';
-- A background merge rebuilds the statistic from live data and would hide the bug. A single-part
-- column-TTL merge is due immediately, so stop TTL merges (which still lets mutations through)
-- before the first insert, and stop merges outright once the last mutation is done.
SYSTEM STOP TTL MERGES ttl_stats_orphan;

-- these rows arrive already expired, so the TTL empties `v` and drops it from the part
INSERT INTO ttl_stats_orphan SELECT now() - INTERVAL 1 DAY, 5 FROM numbers(4);
ALTER TABLE ttl_stats_orphan MATERIALIZE TTL;
-- changing the default makes a stale statistic distinguishable from a correct read
ALTER TABLE ttl_stats_orphan MODIFY COLUMN v UInt32 DEFAULT 99 STATISTICS(basic) TTL d + INTERVAL 1 SECOND;
SYSTEM STOP MERGES ttl_stats_orphan;
-- a second part that really stores a large value
INSERT INTO ttl_stats_orphan SELECT now() + INTERVAL 10 YEAR, 4242 FROM numbers(2);

-- fixture integrity: two level-0 parts, and `v` stored in exactly one of them. A background rewrite
-- of either part would make the assertions below pass without exercising the bug at all.
SELECT
    (SELECT count() FROM system.parts
     WHERE database = currentDatabase() AND table = 'ttl_stats_orphan' AND active),
    (SELECT max(level) FROM system.parts
     WHERE database = currentDatabase() AND table = 'ttl_stats_orphan' AND active),
    (SELECT count() FROM system.parts_columns
     WHERE database = currentDatabase() AND table = 'ttl_stats_orphan' AND active AND column = 'v');

-- the estimate of the part that really stores `v` must survive: dropping every estimate would also
-- satisfy the result assertions below
SELECT `estimates.min`, `estimates.max` FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'ttl_stats_orphan' AND active AND column = 'v';

SELECT arraySort(groupArray(v)) FROM ttl_stats_orphan;
SELECT min(v), max(v) FROM ttl_stats_orphan;
SELECT count() FROM ttl_stats_orphan WHERE v > 50;
SELECT count(), sum(v) FROM ttl_stats_orphan;

-- liveness: statistics part pruning must actually run, otherwise the filtered count above is not an
-- oracle. `v > 5000` is excluded by the statistic of the part that stores 4242.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ttl_stats_orphan WHERE v > 5000)
WHERE explain ILIKE '%Statistics%';

-- liveness: the min/max aggregation must actually be answered from statistics
SELECT count() > 0 FROM (EXPLAIN SELECT min(v), max(v) FROM ttl_stats_orphan)
WHERE explain ILIKE '%_statistics_min_max_projection%';

DROP TABLE ttl_stats_orphan;
