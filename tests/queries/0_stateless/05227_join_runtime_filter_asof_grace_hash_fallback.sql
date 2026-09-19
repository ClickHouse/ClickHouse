-- `GraceHashJoin` never executes an `ASOF` join (see `GraceHashJoin::isSupported`), so it must not be
-- treated as a viable runtime-filter target for one. Before this fix, `supportsRuntimeFilter` accepted
-- `grace_hash` regardless of strictness, so for an `ASOF INNER JOIN` with `join_algorithm =
-- 'grace_hash,full_sorting_merge'` the runtime-filter pass pruned away `full_sorting_merge` (it does not
-- support runtime filters) while keeping `grace_hash` (which "supports" them but can never run the join),
-- leaving `chooseJoinAlgorithm` with no usable algorithm and a `NOT_IMPLEMENTED` exception instead of
-- falling back to `full_sorting_merge` as it did without the runtime-filter feature.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET join_algorithm = 'grace_hash,full_sorting_merge';
SET query_plan_join_swap_table = 0;
SET join_runtime_filter_min_probe_rows = 0;
SET enable_join_runtime_filters = 1;

DROP TABLE IF EXISTS t_rf_asof_grace_left;
DROP TABLE IF EXISTS t_rf_asof_grace_right;

CREATE TABLE t_rf_asof_grace_left (id UInt64, ts UInt64, val UInt64) ENGINE = MergeTree ORDER BY (id, ts);
CREATE TABLE t_rf_asof_grace_right (id UInt64, ts UInt64, val UInt64) ENGINE = MergeTree ORDER BY (id, ts);

INSERT INTO t_rf_asof_grace_left VALUES (1, 10, 100), (1, 20, 200), (2, 5, 50), (3, 15, 150), (4, 8, 80);
INSERT INTO t_rf_asof_grace_right VALUES (1, 5, 1000), (1, 15, 1001), (3, 10, 3000), (3, 20, 3001);

SELECT '--- ASOF INNER JOIN falls back to full_sorting_merge instead of failing ---';

SELECT l.id, l.ts, r.ts, r.val
FROM t_rf_asof_grace_left l ASOF INNER JOIN t_rf_asof_grace_right r ON l.id = r.id AND l.ts >= r.ts
ORDER BY l.id, l.ts;

DROP TABLE t_rf_asof_grace_left;
DROP TABLE t_rf_asof_grace_right;
