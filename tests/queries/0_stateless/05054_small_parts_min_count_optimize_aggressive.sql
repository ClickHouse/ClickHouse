-- The small-parts batching gate (merge_selector_small_parts_min_count) applies only to background
-- merge selection. Aggressive selection (OPTIMIZE without FINAL) is an explicit user request that
-- ignores part novelty, so it must merge a fresh below-min-count small range immediately.

DROP TABLE IF EXISTS t_small_parts_optimize;

CREATE TABLE t_small_parts_optimize (n UInt64) ENGINE = MergeTree ORDER BY n
SETTINGS merge_selector_small_parts_min_count = 8;

INSERT INTO t_small_parts_optimize VALUES (1);
INSERT INTO t_small_parts_optimize VALUES (2);
INSERT INTO t_small_parts_optimize VALUES (3);

-- Without the aggressive-path exemption the gate rejects this fresh 3-part range and OPTIMIZE is a no-op.
OPTIMIZE TABLE t_small_parts_optimize SETTINGS optimize_throw_if_noop = 1;

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_small_parts_optimize' AND active;
SELECT sum(n) FROM t_small_parts_optimize;

DROP TABLE t_small_parts_optimize;
