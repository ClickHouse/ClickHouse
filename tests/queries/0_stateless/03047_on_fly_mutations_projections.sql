-- Tags: no-parallel-replicas
-- no-parallel-replicas: projections don't work with parallel replicas.

DROP TABLE IF EXISTS t_update_projections;

SET apply_mutations_on_fly = 1;

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
