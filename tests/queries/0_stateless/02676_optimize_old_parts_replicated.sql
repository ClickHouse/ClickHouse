-- Tags: long

DROP TABLE IF EXISTS test_without_merge;
DROP TABLE IF EXISTS test_replicated;

SELECT 'Without merge';

CREATE TABLE test_without_merge (i Int64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test02676_without_merge', 'node')  ORDER BY i;
INSERT INTO test_without_merge SELECT 1;
INSERT INTO test_without_merge SELECT 2;
INSERT INTO test_without_merge SELECT 3;

SELECT sleepEachRow(1) FROM numbers(9) FORMAT Null;
SELECT count(*) FROM system.parts WHERE database = currentDatabase() AND table='test_without_merge' AND active;

DROP TABLE test_without_merge;

SELECT 'With merge replicated any part range';

CREATE TABLE test_replicated (i Int64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test02676', 'node')  ORDER BY i
SETTINGS min_age_to_force_merge_seconds=3, min_age_to_force_merge_on_partition_only=false;
INSERT INTO test_replicated SELECT 1;
INSERT INTO test_replicated SELECT 2;
INSERT INTO test_replicated SELECT 3;

SELECT sleepEachRow(1) FROM numbers(9) FORMAT Null;
SELECT count(*) FROM system.parts WHERE database = currentDatabase() AND table='test_replicated' AND active;

DROP TABLE test_replicated;

SELECT 'With merge replicated partition only';

CREATE TABLE test_replicated (i Int64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test02676_partition_only', 'node')  ORDER BY i
SETTINGS min_age_to_force_merge_seconds=3, min_age_to_force_merge_on_partition_only=true;
INSERT INTO test_replicated SELECT 1;
INSERT INTO test_replicated SELECT 2;
INSERT INTO test_replicated SELECT 3;

SELECT sleepEachRow(1) FROM numbers(9) FORMAT Null;
SELECT count(*) FROM system.parts WHERE database = currentDatabase() AND table='test_replicated' AND active;

SELECT sleepEachRow(1) FROM numbers(9) FORMAT Null;
SELECT (now() - modification_time) > 5 FROM system.parts WHERE database = currentDatabase() AND table='test_replicated' AND active;

DROP TABLE test_replicated;
