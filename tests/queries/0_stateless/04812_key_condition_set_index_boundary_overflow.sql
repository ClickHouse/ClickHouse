-- An excluded key range boundary at a key column type's maximum is shrunk to one above the domain.
-- Storing it used to truncate it back into the domain, where it could equal a set element, so the
-- primary key index selected granules that cannot hold a match.

-- The query condition cache would prune granules before the primary key index runs.
SET use_query_condition_cache = 0;
-- A remote replica may skip index analysis and report full ranges.
SET parallel_replicas_index_analysis_only_on_coordinator = 0;
-- Exact ranges, which the index consistency check verifies, are only requested for this count().
SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 1;
-- Column statistics would reject the predicate before the primary key index runs.
SET use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_setidx;
CREATE TABLE t_setidx (a UInt64, b UInt8, c String) ENGINE = MergeTree ORDER BY (a, b, c)
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_setidx SELECT number % 5, 255, toString(number) FROM numbers(20);

-- Every value of b is 255, so no granule can match: the index must select a single granule of one
-- row. Before the fix it selected five: a debug build aborted, a release build read too many rows.
SELECT count() FROM t_setidx WHERE a = 1 AND b IN (0) SETTINGS max_rows_to_read = 1;

DROP TABLE t_setidx;
