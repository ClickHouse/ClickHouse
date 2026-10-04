-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree
-- A replica fetches parts that have a projection its table metadata does not have yet. Merges and mutations
-- on these parts must keep the projection correct after the replica applies the ALTER that adds it.

DROP TABLE IF EXISTS r1;
DROP TABLE IF EXISTS r2;

CREATE TABLE r1 (g UInt8, k UInt32, i UInt32, s UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r1') PARTITION BY g ORDER BY k
SETTINGS index_granularity = 16, lightweight_mutation_projection_mode = 'rebuild';
CREATE TABLE r2 (g UInt8, k UInt32, i UInt32, s UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r2') PARTITION BY g ORDER BY k
SETTINGS index_granularity = 16, lightweight_mutation_projection_mode = 'rebuild';

SET alter_sync = 1;

INSERT INTO r1 VALUES (0, 0, 0, 0);
SYSTEM SYNC REPLICA r2;

-- r2 cannot run the mutation of the data alter, so its ALTER that adds the projection waits behind it.
SYSTEM STOP MERGES r2;
ALTER TABLE r1 MODIFY COLUMN s UInt64;
ALTER TABLE r1 ADD PROJECTION pr (SELECT g, k, i ORDER BY i), ADD PROJECTION pa (SELECT g, count() GROUP BY g);

INSERT INTO r1 SELECT 1, number, number, 0 FROM numbers(100);
INSERT INTO r1 SELECT 2, number, number, 0 FROM numbers(100);
INSERT INTO r1 SELECT 3, number, number, 0 FROM numbers(100);
INSERT INTO r1 SELECT 4, number, number, 0 FROM numbers(100);

-- r2 fetches the new parts before it has the projection.
SYSTEM SYNC REPLICA r2 LIGHTWEIGHT;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'r2' AND active AND partition != '0';
SELECT countIf(type = 'ALTER_METADATA') > 0 FROM system.replication_queue WHERE database = currentDatabase() AND table = 'r2';

SYSTEM START MERGES r2;
SYSTEM SYNC REPLICA r2;
-- r2 executes the merges and mutations below itself.
SYSTEM STOP MERGES r1;

SELECT 'merge';
OPTIMIZE TABLE r2 PARTITION 1 FINAL;
SYSTEM SYNC REPLICA r2;
SELECT name, rows FROM system.projection_parts WHERE database = currentDatabase() AND table = 'r2' AND active AND name = 'pr' AND partition = '1';

SELECT 'update of a projection column';
ALTER TABLE r2 UPDATE i = i + 1000 IN PARTITION 2 WHERE 1 SETTINGS mutations_sync = 1;
SELECT count() FROM r2 WHERE i = 1010 AND g = 2 SETTINGS optimize_use_projections = 1, force_optimize_projection_name = 'pr', enable_parallel_replicas = 0;
SELECT count() FROM r2 WHERE i = 1010 AND g = 2 SETTINGS optimize_use_projections = 0;

SELECT 'update of another column';
ALTER TABLE r2 UPDATE s = 1 IN PARTITION 3 WHERE 1 SETTINGS mutations_sync = 1;
SELECT count() FROM r2 WHERE i = 10 AND g = 3 SETTINGS optimize_use_projections = 1, force_optimize_projection_name = 'pr', enable_parallel_replicas = 0;
SELECT name, rows FROM system.projection_parts WHERE database = currentDatabase() AND table = 'r2' AND active AND name = 'pr' AND partition = '3';

SELECT 'lightweight delete';
DELETE FROM r2 IN PARTITION 4 WHERE k < 50 SETTINGS lightweight_deletes_sync = 1;
SELECT count() FROM r2 WHERE g = 4 SETTINGS optimize_use_projections = 0;
-- A rebuilt normal projection still returns the deleted rows (#111791), so pr is checked for presence and a remaining row, and the aggregate projection pa for the delete.
SELECT name FROM system.projection_parts WHERE database = currentDatabase() AND table = 'r2' AND active AND name = 'pr' AND partition = '4';
SELECT count() FROM r2 WHERE i = 60 AND g = 4 SETTINGS optimize_use_projections = 1, force_optimize_projection_name = 'pr', enable_parallel_replicas = 0;
SELECT g, count() FROM r2 WHERE g = 4 GROUP BY g SETTINGS optimize_use_projections = 1, force_optimize_projection_name = 'pa', enable_parallel_replicas = 0, optimize_aggregation_in_order = 0;

SELECT 'r1';
SYSTEM START MERGES r1;
SYSTEM SYNC REPLICA r1;
SELECT name, rows FROM system.projection_parts WHERE database = currentDatabase() AND table = 'r1' AND active AND name = 'pr' AND partition = '1';
SELECT count() FROM r1 WHERE i = 1010 AND g = 2 SETTINGS optimize_use_projections = 1, force_optimize_projection_name = 'pr', enable_parallel_replicas = 0;
SELECT count() FROM r1 WHERE i = 1010 AND g = 2 SETTINGS optimize_use_projections = 0;
SELECT count() FROM r1 WHERE i = 10 AND g = 3 SETTINGS optimize_use_projections = 1, force_optimize_projection_name = 'pr', enable_parallel_replicas = 0;
SELECT name, rows FROM system.projection_parts WHERE database = currentDatabase() AND table = 'r1' AND active AND name = 'pr' AND partition = '3';
SELECT count() FROM r1 WHERE g = 4 SETTINGS optimize_use_projections = 0;
SELECT name FROM system.projection_parts WHERE database = currentDatabase() AND table = 'r1' AND active AND name = 'pr' AND partition = '4';
SELECT count() FROM r1 WHERE i = 60 AND g = 4 SETTINGS optimize_use_projections = 1, force_optimize_projection_name = 'pr', enable_parallel_replicas = 0;
SELECT g, count() FROM r1 WHERE g = 4 GROUP BY g SETTINGS optimize_use_projections = 1, force_optimize_projection_name = 'pa', enable_parallel_replicas = 0, optimize_aggregation_in_order = 0;

DROP TABLE r1;
DROP TABLE r2;
