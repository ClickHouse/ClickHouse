-- A column TTL that empties a column must not leave a stale statistic behind when MATERIALIZE TTL
-- rewrites only the TTL column and carries the other columns of the part over.

DROP TABLE IF EXISTS ttl_stats_orphan_partial;
SET mutations_sync = 2, alter_sync = 2, materialize_statistics_on_insert = 1;
SET use_statistics = 1, use_statistics_for_part_pruning = 1, use_statistics_for_min_max_aggregation = 1;
SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SET optimize_aggregation_in_order = 0, force_aggregation_in_order = 0, aggregate_functions_null_for_empty = 0;
SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET automatic_parallel_replicas_mode = 0, enable_parallel_replicas = 0;

DROP TABLE IF EXISTS ttl_stats_orphan_partial;
-- wide parts in full storage: otherwise MATERIALIZE TTL rewrites every column
CREATE TABLE ttl_stats_orphan_partial (k UInt64, d DateTime, v UInt32 DEFAULT 7 STATISTICS(basic) TTL d + INTERVAL 1 SECOND)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, auto_statistics_types = 'basic';
SYSTEM STOP TTL MERGES ttl_stats_orphan_partial;

INSERT INTO ttl_stats_orphan_partial SELECT number, toDateTime('2000-01-01 00:00:00'), 5 FROM numbers(4);
ALTER TABLE ttl_stats_orphan_partial MATERIALIZE TTL;
ALTER TABLE ttl_stats_orphan_partial MODIFY COLUMN v UInt32 DEFAULT 99 STATISTICS(basic) TTL d + INTERVAL 1 SECOND;
SYSTEM STOP MERGES ttl_stats_orphan_partial;
INSERT INTO ttl_stats_orphan_partial SELECT number + 4, toDateTime('2100-01-01 00:00:00'), 4242 FROM numbers(2);

-- two level-0 parts, `v` stored in exactly one of them
SELECT
    (SELECT count() FROM system.parts
     WHERE database = currentDatabase() AND table = 'ttl_stats_orphan_partial' AND active),
    (SELECT max(level) FROM system.parts
     WHERE database = currentDatabase() AND table = 'ttl_stats_orphan_partial' AND active),
    (SELECT count() FROM system.parts_columns
     WHERE database = currentDatabase() AND table = 'ttl_stats_orphan_partial' AND active AND column = 'v');

-- the carried-over estimate of `k` must survive the mutation
SELECT column, `estimates.min`, `estimates.max` FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'ttl_stats_orphan_partial' AND active AND column IN ('k', 'v')
ORDER BY column, `estimates.min`;

SELECT arraySort(groupArray(v)) FROM ttl_stats_orphan_partial;
SELECT min(v), max(v) FROM ttl_stats_orphan_partial;
SELECT count() FROM ttl_stats_orphan_partial WHERE v > 50;

SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ttl_stats_orphan_partial WHERE v > 5000)
WHERE explain ILIKE '%Statistics%';
SELECT count() > 0 FROM (EXPLAIN SELECT min(v), max(v) FROM ttl_stats_orphan_partial)
WHERE explain ILIKE '%_statistics_min_max_projection%';

DROP TABLE ttl_stats_orphan_partial;
