-- Result-size limits (max_result_rows / max_result_bytes / extremes) must be enforced only at the
-- outer query boundary, not on the underlying distributed read of a trivial view. StorageView::
-- readImpl disables them for the inner view query (getViewContext); the pushdown must do the same.
-- Otherwise `SELECT id FROM v ORDER BY id LIMIT 1 SETTINGS max_result_rows = 1` throws on a shard
-- whose intermediate read exceeds the limit, before the coordinator applies the final ORDER BY/LIMIT
-- — even though the non-pushdown path returns one row.
--
-- distributed_push_down_limit = 0 makes the shards send all rows to the coordinator (rather than a
-- pre-limited result), so the shard's intermediate result genuinely exceeds max_result_rows and the
-- divergence is observable.
--
-- Tags: distributed

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET prefer_localhost_replica = 0;
SET distributed_push_down_limit = 0;

DROP TABLE IF EXISTS 04368_local;
DROP TABLE IF EXISTS 04368_dist;
DROP VIEW IF EXISTS 04368_view;

CREATE TABLE 04368_local (id UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE 04368_dist AS 04368_local
ENGINE = Distributed(test_shard_localhost, currentDatabase(), 04368_local);
CREATE VIEW 04368_view AS SELECT id FROM 04368_dist;

INSERT INTO 04368_dist VALUES (1), (2), (3), (4), (5);
SYSTEM FLUSH DISTRIBUTED 04368_dist;

-- Pushdown ON: max_result_rows applies only to the final (1-row) result, so the query returns one
-- row even though the shard read produces five rows.
SELECT id FROM 04368_view ORDER BY id LIMIT 1
SETTINGS optimize_trivial_view_pushdown_to_distributed = 1, max_result_rows = 1, result_overflow_mode = 'throw';

-- Pushdown OFF: same result (reference behavior).
SELECT id FROM 04368_view ORDER BY id LIMIT 1
SETTINGS optimize_trivial_view_pushdown_to_distributed = 0, max_result_rows = 1, result_overflow_mode = 'throw';

-- Guard against over-disabling: without an outer LIMIT the final result has five rows, so the limit
-- must still fire at the outer boundary on both paths (we only moved enforcement, not removed it).
SELECT id FROM 04368_view ORDER BY id
SETTINGS optimize_trivial_view_pushdown_to_distributed = 1, max_result_rows = 1, result_overflow_mode = 'throw'; -- { serverError 396 }
SELECT id FROM 04368_view ORDER BY id
SETTINGS optimize_trivial_view_pushdown_to_distributed = 0, max_result_rows = 1, result_overflow_mode = 'throw'; -- { serverError 396 }

DROP VIEW 04368_view;
DROP TABLE 04368_dist;
DROP TABLE 04368_local;
