-- Tags: no-shared-catalog
-- FIXME no-shared-catalog: STOP MERGES will only stop them on the current replica, the second one will continue to merge

DROP TABLE IF EXISTS t_update_materialized;

SET apply_mutations_on_fly = 1;

CREATE TABLE t_update_materialized (id UInt64, c1 UInt64, c2 UInt64 MATERIALIZED c1 * 2)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_update_materialized', '1') ORDER BY id;

SYSTEM STOP MERGES t_update_materialized;

INSERT INTO t_update_materialized (id, c1) VALUES (1, 1);

SELECT id, c2 FROM t_update_materialized ORDER BY id;
SELECT id, c1, c2 FROM t_update_materialized ORDER BY id;

ALTER TABLE t_update_materialized UPDATE c1 = 2 WHERE id = 1;

SYSTEM SYNC REPLICA t_update_materialized PULL;

SELECT id, c2 FROM t_update_materialized;
SELECT id, c1, c2 FROM t_update_materialized;

SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_update_materialized' AND NOT is_done;
