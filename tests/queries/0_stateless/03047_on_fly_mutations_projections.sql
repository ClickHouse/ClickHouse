
DROP TABLE IF EXISTS t_update_projections;

SET apply_mutations_on_fly = 1;
SET parallel_replicas_local_plan = 1, parallel_replicas_support_projection = 1, optimize_aggregation_in_order = 0;

CREATE TABLE t_update_projections (id UInt64, v UInt64, PROJECTION proj (SELECT sum(v)))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_update_projections', '1') ORDER BY tuple();

SYSTEM STOP MERGES t_update_projections;

INSERT INTO t_update_projections SELECT number, number FROM numbers(100000);
SELECT sum(v) FROM t_update_projections SETTINGS force_optimize_projection = 1;

ALTER TABLE t_update_projections UPDATE v = v * v WHERE id % 2 = 1;

SYSTEM SYNC REPLICA t_update_projections PULL;

SELECT sum(v) FROM t_update_projections;
SELECT sum(v) FROM t_update_projections SETTINGS force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }

DROP TABLE t_update_projections;
