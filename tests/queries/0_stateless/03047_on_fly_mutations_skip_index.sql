-- Tags: no-shared-catalog
-- FIXME no-shared-catalog: STOP MERGES will only stop them on the current replica, the second one will continue to merge

DROP TABLE IF EXISTS t_lightweight_mut_3;

SET apply_mutations_on_fly = 1;

CREATE TABLE t_lightweight_mut_3 (id UInt64, v UInt64, INDEX idx v TYPE minmax GRANULARITY 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lightweight_mut_3', '1') ORDER BY id;

SYSTEM STOP MERGES t_lightweight_mut_3;

INSERT INTO t_lightweight_mut_3 VALUES (1, 1);
INSERT INTO t_lightweight_mut_3 VALUES (2, 2000);

SELECT id, v FROM t_lightweight_mut_3 WHERE v > 100 ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

ALTER TABLE t_lightweight_mut_3 UPDATE v = 1000 WHERE id = 1;

SYSTEM SYNC REPLICA t_lightweight_mut_3 PULL;

SELECT id, v FROM t_lightweight_mut_3 WHERE v > 100 ORDER BY id;
SELECT id, v FROM t_lightweight_mut_3 WHERE v > 100 ORDER BY id SETTINGS force_data_skipping_indices = 'idx', apply_mutations_on_fly = 0;
SELECT id, v FROM t_lightweight_mut_3 WHERE v > 100 ORDER BY id SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE t_lightweight_mut_3;
