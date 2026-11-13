-- Parts whose statistics were materialized before `basic` became the statistics type carry
-- min/max but no NULL count, so `IS NULL` / `IS NOT NULL` must not prune them.

DROP TABLE IF EXISTS t_stats_stale_null_count;
SET allow_statistics = 1;
SET use_statistics_for_part_pruning = 1;
SET materialize_statistics_on_insert = 1;
SET optimize_functions_to_subcolumns = 1;
SET mutations_sync = 2;

DROP TABLE IF EXISTS t_stats_stale_null_count;

-- `nullable_serialization_version` is pinned because with sparse serialization
-- `count() ... WHERE col IS NULL` is answered without reading the part, and EXPLAIN then has no
-- index sections at all.
CREATE TABLE t_stats_stale_null_count (bucket UInt8, value Nullable(Int64))
ENGINE = MergeTree
PARTITION BY bucket
ORDER BY tuple()
SETTINGS auto_statistics_types = 'minmax', nullable_serialization_version = 'basic';

INSERT INTO t_stats_stale_null_count VALUES (0, NULL), (0, NULL);
INSERT INTO t_stats_stale_null_count VALUES (1, 100), (1, 101);

ALTER TABLE t_stats_stale_null_count MODIFY SETTING auto_statistics_types = 'basic';

SELECT arraySort(groupUniqArray(arrayJoin(statistics)))
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stats_stale_null_count' AND active AND column = 'value';

SELECT count() FROM t_stats_stale_null_count WHERE value IS NULL;
SELECT count() FROM t_stats_stale_null_count WHERE value IS NOT NULL;

SELECT countIf(explain LIKE '%Statistics%')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_stats_stale_null_count WHERE value IS NULL);
SELECT countIf(explain LIKE '%Statistics%')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_stats_stale_null_count WHERE value IS NOT NULL);

-- The old min/max still prunes, so the per-part statistics are loaded: only the NULL count is missing.
SELECT countIf(explain LIKE '%Statistics%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_stats_stale_null_count WHERE value < 50);

ALTER TABLE t_stats_stale_null_count MATERIALIZE STATISTICS ALL;

SELECT arraySort(groupUniqArray(arrayJoin(statistics)))
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stats_stale_null_count' AND active AND column = 'value';

SELECT countIf(explain LIKE '%Statistics%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_stats_stale_null_count WHERE value IS NULL);
SELECT count() FROM t_stats_stale_null_count WHERE value IS NULL;
SELECT count() FROM t_stats_stale_null_count WHERE value IS NOT NULL;

DROP TABLE t_stats_stale_null_count;
