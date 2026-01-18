-- Tags: no-shared-catalog, no-parallel-replicas
-- no-shared-catalog: STOP MERGES will only stop them on the current replica, the second one will continue to merge
-- no-parallel-replicas: the result of EXPLAIN differs with parallel replicas

SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_lightweight_mut_3;

SET mutations_sync = 0;

CREATE TABLE t_lightweight_mut_3 (id UInt64, v UInt64, INDEX idx v TYPE minmax GRANULARITY 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lightweight_mut_3', '1') ORDER BY id;

SYSTEM STOP MERGES t_lightweight_mut_3;

INSERT INTO t_lightweight_mut_3 VALUES (1, 1);
INSERT INTO t_lightweight_mut_3 VALUES (2, 2000);

SELECT id, v FROM t_lightweight_mut_3 WHERE v > 100 ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

SELECT trim(explain) AS s FROM (
    EXPLAIN indexes = 1
    SELECT id, v FROM t_lightweight_mut_3 WHERE v > 100 ORDER BY id SETTINGS force_data_skipping_indices = 'idx'
) WHERE s LIKE 'Granules: %';

ALTER TABLE t_lightweight_mut_3 UPDATE v = 1000 WHERE id = 1;
INSERT INTO t_lightweight_mut_3 VALUES (3, 3);

SYSTEM SYNC REPLICA t_lightweight_mut_3 PULL;

SELECT id, v FROM t_lightweight_mut_3 WHERE v > 100 ORDER BY id SETTINGS apply_mutations_on_fly = 1;

SELECT trim(explain) AS s FROM (
    EXPLAIN indexes = 1
    SELECT id, v FROM t_lightweight_mut_3 WHERE v > 100 ORDER BY id SETTINGS apply_mutations_on_fly = 1
) WHERE s LIKE 'Granules: %';

SELECT id, v FROM t_lightweight_mut_3 WHERE v > 100 ORDER BY id SETTINGS apply_mutations_on_fly = 0;

SELECT trim(explain) AS s FROM (
    EXPLAIN indexes = 1
    SELECT id, v FROM t_lightweight_mut_3 WHERE v > 100 ORDER BY id SETTINGS apply_mutations_on_fly = 0
) WHERE s LIKE 'Granules: %';

DROP TABLE t_lightweight_mut_3;
