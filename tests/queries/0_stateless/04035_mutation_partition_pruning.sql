-- Tags: long, no-replicated-database, no-parallel
-- Test automatic partition pruning for mutations.
-- This optimization works for ReplicatedMergeTree (and SharedMergeTree)
-- because they use partition-based block number allocation.

SET mutations_sync = 1;

-- ============================================================
-- Phase 1: Automatic WHERE-based partition pruning
-- ============================================================

-- Test 1: DELETE with exact partition key match - should prune to 1 partition
DROP TABLE IF EXISTS t_mut_prune_1;
CREATE TABLE t_mut_prune_1 (key UInt64, value String, dt Date)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mut_prune_1', '1')
PARTITION BY dt ORDER BY key;

INSERT INTO t_mut_prune_1 VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-01');
INSERT INTO t_mut_prune_1 VALUES (3, 'c', '2024-01-02'), (4, 'd', '2024-01-02');
INSERT INTO t_mut_prune_1 VALUES (5, 'e', '2024-01-03'), (6, 'f', '2024-01-03');

ALTER TABLE t_mut_prune_1 DELETE WHERE dt = '2024-01-01';

SELECT 'test 1: delete with exact partition match';
SELECT * FROM t_mut_prune_1 ORDER BY key;

SELECT 'test 1: affected partitions';
SELECT arraySort(block_numbers.partition_id) as partitions
FROM system.mutations
WHERE database = currentDatabase() AND table = 't_mut_prune_1' AND NOT is_killed
ORDER BY mutation_id;

DROP TABLE t_mut_prune_1 SYNC;

-- Test 2: UPDATE with exact partition key match - should prune to 1 partition
DROP TABLE IF EXISTS t_mut_prune_2;
CREATE TABLE t_mut_prune_2 (key UInt64, value String, dt Date)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mut_prune_2', '1')
PARTITION BY dt ORDER BY key;

INSERT INTO t_mut_prune_2 VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-01');
INSERT INTO t_mut_prune_2 VALUES (3, 'c', '2024-01-02'), (4, 'd', '2024-01-02');
INSERT INTO t_mut_prune_2 VALUES (5, 'e', '2024-01-03'), (6, 'f', '2024-01-03');

ALTER TABLE t_mut_prune_2 UPDATE value = 'updated' WHERE dt = '2024-01-02';

SELECT 'test 2: update with exact partition match';
SELECT * FROM t_mut_prune_2 ORDER BY key;

SELECT 'test 2: affected partitions';
SELECT arraySort(block_numbers.partition_id) as partitions
FROM system.mutations
WHERE database = currentDatabase() AND table = 't_mut_prune_2' AND NOT is_killed
ORDER BY mutation_id;

DROP TABLE t_mut_prune_2 SYNC;

-- Test 3: DELETE with IN condition - should prune to 2 partitions
DROP TABLE IF EXISTS t_mut_prune_3;
CREATE TABLE t_mut_prune_3 (key UInt64, value String, dt Date)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mut_prune_3', '1')
PARTITION BY dt ORDER BY key;

INSERT INTO t_mut_prune_3 VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-01');
INSERT INTO t_mut_prune_3 VALUES (3, 'c', '2024-01-02'), (4, 'd', '2024-01-02');
INSERT INTO t_mut_prune_3 VALUES (5, 'e', '2024-01-03'), (6, 'f', '2024-01-03');

ALTER TABLE t_mut_prune_3 DELETE WHERE dt IN ('2024-01-01', '2024-01-02');

SELECT 'test 3: delete with IN on partition key';
SELECT * FROM t_mut_prune_3 ORDER BY key;

SELECT 'test 3: affected partitions';
SELECT arraySort(block_numbers.partition_id) as partitions
FROM system.mutations
WHERE database = currentDatabase() AND table = 't_mut_prune_3' AND NOT is_killed
ORDER BY mutation_id;

DROP TABLE t_mut_prune_3 SYNC;

-- Test 4: Mixed condition (partition + non-partition) - should prune to 1 partition
DROP TABLE IF EXISTS t_mut_prune_4;
CREATE TABLE t_mut_prune_4 (key UInt64, value String, dt Date)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mut_prune_4', '1')
PARTITION BY dt ORDER BY key;

INSERT INTO t_mut_prune_4 VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-01');
INSERT INTO t_mut_prune_4 VALUES (3, 'c', '2024-01-02'), (4, 'd', '2024-01-02');
INSERT INTO t_mut_prune_4 VALUES (5, 'e', '2024-01-03'), (6, 'f', '2024-01-03');

ALTER TABLE t_mut_prune_4 DELETE WHERE dt = '2024-01-01' AND key > 1;

SELECT 'test 4: mixed condition';
SELECT * FROM t_mut_prune_4 ORDER BY key;

SELECT 'test 4: affected partitions';
SELECT arraySort(block_numbers.partition_id) as partitions
FROM system.mutations
WHERE database = currentDatabase() AND table = 't_mut_prune_4' AND NOT is_killed
ORDER BY mutation_id;

DROP TABLE t_mut_prune_4 SYNC;

-- Test 5: Non-partition condition - should NOT prune (all partitions affected)
DROP TABLE IF EXISTS t_mut_prune_5;
CREATE TABLE t_mut_prune_5 (key UInt64, value String, dt Date)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mut_prune_5', '1')
PARTITION BY dt ORDER BY key;

INSERT INTO t_mut_prune_5 VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-01');
INSERT INTO t_mut_prune_5 VALUES (3, 'c', '2024-01-02'), (4, 'd', '2024-01-02');
INSERT INTO t_mut_prune_5 VALUES (5, 'e', '2024-01-03'), (6, 'f', '2024-01-03');

ALTER TABLE t_mut_prune_5 UPDATE value = 'all' WHERE key > 0;

SELECT 'test 5: non-partition condition (all partitions)';
SELECT * FROM t_mut_prune_5 ORDER BY key;

SELECT 'test 5: affected partitions';
SELECT arraySort(block_numbers.partition_id) as partitions
FROM system.mutations
WHERE database = currentDatabase() AND table = 't_mut_prune_5' AND NOT is_killed
ORDER BY mutation_id;

DROP TABLE t_mut_prune_5 SYNC;

-- Test 6: Range condition on partition key
DROP TABLE IF EXISTS t_mut_prune_6;
CREATE TABLE t_mut_prune_6 (key UInt64, value String, dt Date)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mut_prune_6', '1')
PARTITION BY dt ORDER BY key;

INSERT INTO t_mut_prune_6 VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-01');
INSERT INTO t_mut_prune_6 VALUES (3, 'c', '2024-01-02'), (4, 'd', '2024-01-02');
INSERT INTO t_mut_prune_6 VALUES (5, 'e', '2024-01-03'), (6, 'f', '2024-01-03');

ALTER TABLE t_mut_prune_6 DELETE WHERE dt >= '2024-01-02';

SELECT 'test 6: range condition';
SELECT * FROM t_mut_prune_6 ORDER BY key;

SELECT 'test 6: affected partitions';
SELECT arraySort(block_numbers.partition_id) as partitions
FROM system.mutations
WHERE database = currentDatabase() AND table = 't_mut_prune_6' AND NOT is_killed
ORDER BY mutation_id;

DROP TABLE t_mut_prune_6 SYNC;

-- Test 7: Setting disabled - should NOT prune
DROP TABLE IF EXISTS t_mut_prune_7;
CREATE TABLE t_mut_prune_7 (key UInt64, value String, dt Date)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mut_prune_7', '1')
PARTITION BY dt ORDER BY key;

INSERT INTO t_mut_prune_7 VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-01');
INSERT INTO t_mut_prune_7 VALUES (3, 'c', '2024-01-02'), (4, 'd', '2024-01-02');

ALTER TABLE t_mut_prune_7 DELETE WHERE dt = '2024-01-02'
    SETTINGS optimize_mutations_with_partition_pruning = 0;

SELECT 'test 7: optimization disabled';
SELECT * FROM t_mut_prune_7 ORDER BY key;

SELECT 'test 7: affected partitions (should be all)';
SELECT arraySort(block_numbers.partition_id) as partitions
FROM system.mutations
WHERE database = currentDatabase() AND table = 't_mut_prune_7' AND NOT is_killed
ORDER BY mutation_id;

DROP TABLE t_mut_prune_7 SYNC;

-- ============================================================
-- Phase 2: Multi-partition IN PARTITION syntax
-- ============================================================

-- Test 8: DELETE with multi-partition IN PARTITION
DROP TABLE IF EXISTS t_mut_multi_1;
CREATE TABLE t_mut_multi_1 (key UInt64, value String, dt Date)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mut_multi_1', '1')
PARTITION BY dt ORDER BY key;

INSERT INTO t_mut_multi_1 VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-01');
INSERT INTO t_mut_multi_1 VALUES (3, 'c', '2024-01-02'), (4, 'd', '2024-01-02');
INSERT INTO t_mut_multi_1 VALUES (5, 'e', '2024-01-03'), (6, 'f', '2024-01-03');

ALTER TABLE t_mut_multi_1 DELETE IN PARTITION '2024-01-01', '2024-01-02' WHERE key > 1;

SELECT 'test 8: multi-partition delete';
SELECT * FROM t_mut_multi_1 ORDER BY key;

DROP TABLE t_mut_multi_1 SYNC;

-- Test 9: UPDATE with multi-partition IN PARTITION
DROP TABLE IF EXISTS t_mut_multi_2;
CREATE TABLE t_mut_multi_2 (key UInt64, value String, dt Date)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mut_multi_2', '1')
PARTITION BY dt ORDER BY key;

INSERT INTO t_mut_multi_2 VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-01');
INSERT INTO t_mut_multi_2 VALUES (3, 'c', '2024-01-02'), (4, 'd', '2024-01-02');
INSERT INTO t_mut_multi_2 VALUES (5, 'e', '2024-01-03'), (6, 'f', '2024-01-03');

ALTER TABLE t_mut_multi_2 UPDATE value = 'upd' IN PARTITION '2024-01-01', '2024-01-03' WHERE 1;

SELECT 'test 9: multi-partition update';
SELECT * FROM t_mut_multi_2 ORDER BY key;

DROP TABLE t_mut_multi_2 SYNC;

-- Test 10: Multi-partition with partition ID syntax
DROP TABLE IF EXISTS t_mut_multi_3;
CREATE TABLE t_mut_multi_3 (key UInt64, value String, dt Date)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mut_multi_3', '1')
PARTITION BY dt ORDER BY key;

INSERT INTO t_mut_multi_3 VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-01');
INSERT INTO t_mut_multi_3 VALUES (3, 'c', '2024-01-02'), (4, 'd', '2024-01-02');
INSERT INTO t_mut_multi_3 VALUES (5, 'e', '2024-01-03'), (6, 'f', '2024-01-03');

ALTER TABLE t_mut_multi_3 DELETE IN PARTITION ID '20240101', ID '20240103' WHERE 1;

SELECT 'test 10: multi-partition ID delete';
SELECT * FROM t_mut_multi_3 ORDER BY key;

DROP TABLE t_mut_multi_3 SYNC;

-- ============================================================
-- Phase 3: Pruning of DateTime predicates rewritten for the session timezone
-- ============================================================
-- Each table holds two rows per partition at the two instants the same literal denotes:
-- key 1 in the session timezone and key 9 in UTC. The assertions therefore hold whatever
-- timezone the server runs in. Midday mid-month keeps every row in its own month.

-- Test 11: a literal list rewritten with the session timezone still prunes to 1 partition
DROP TABLE IF EXISTS t_mut_prune_11;
CREATE TABLE t_mut_prune_11 (key UInt64, time DateTime)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mut_prune_11', '1')
PARTITION BY toYYYYMM(time) ORDER BY key;

SET session_timezone = 'America/Denver';

INSERT INTO t_mut_prune_11 SELECT number * 10 + 1, toDateTime(concat('2024-', lpad(toString(number * 5 + 1), 2, '0'), '-15 12:00:00')) FROM numbers(3);
INSERT INTO t_mut_prune_11 SELECT number * 10 + 9, toDateTime(concat('2024-', lpad(toString(number * 5 + 1), 2, '0'), '-15 12:00:00'), 'UTC') FROM numbers(3);

SELECT 'test 11: rows the identical SELECT matches';
SELECT key FROM t_mut_prune_11 WHERE time IN ('2024-06-15 12:00:00') ORDER BY key;

ALTER TABLE t_mut_prune_11 DELETE WHERE time IN ('2024-06-15 12:00:00');

SELECT 'test 11: survivors';
SELECT key FROM t_mut_prune_11 ORDER BY key;

SELECT 'test 11: affected partitions';
SELECT arraySort(block_numbers.partition_id) as partitions
FROM system.mutations
WHERE database = currentDatabase() AND table = 't_mut_prune_11' AND NOT is_killed
ORDER BY mutation_id;

DROP TABLE t_mut_prune_11 SYNC;

-- Test 12: a subquery right-hand side is a prepared set, so it must still not prune
DROP TABLE IF EXISTS t_mut_prune_12;
CREATE TABLE t_mut_prune_12 (key UInt64, time DateTime)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mut_prune_12', '1')
PARTITION BY toYYYYMM(time) ORDER BY key;

INSERT INTO t_mut_prune_12 SELECT number * 10 + 1, toDateTime(concat('2024-', lpad(toString(number * 5 + 1), 2, '0'), '-15 12:00:00')) FROM numbers(3);
INSERT INTO t_mut_prune_12 SELECT number * 10 + 9, toDateTime(concat('2024-', lpad(toString(number * 5 + 1), 2, '0'), '-15 12:00:00'), 'UTC') FROM numbers(3);

ALTER TABLE t_mut_prune_12 DELETE WHERE time IN (SELECT toDateTime('2024-06-15 12:00:00')) SETTINGS allow_nondeterministic_mutations = 1;

SELECT 'test 12: affected partitions (should be all)';
SELECT arraySort(block_numbers.partition_id) as partitions
FROM system.mutations
WHERE database = currentDatabase() AND table = 't_mut_prune_12' AND NOT is_killed
ORDER BY mutation_id;

DROP TABLE t_mut_prune_12 SYNC;

-- Test 13: a cast to a DateTime array that pins no timezone denotes different instants on
-- servers in different timezones, so it must still not prune
DROP TABLE IF EXISTS t_mut_prune_13;
CREATE TABLE t_mut_prune_13 (key UInt64, time DateTime)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mut_prune_13', '1')
PARTITION BY toYYYYMM(time) ORDER BY key;

INSERT INTO t_mut_prune_13 SELECT number * 10 + 1, toDateTime(concat('2024-', lpad(toString(number * 5 + 1), 2, '0'), '-15 12:00:00')) FROM numbers(3);
INSERT INTO t_mut_prune_13 SELECT number * 10 + 9, toDateTime(concat('2024-', lpad(toString(number * 5 + 1), 2, '0'), '-15 12:00:00'), 'UTC') FROM numbers(3);

ALTER TABLE t_mut_prune_13 DELETE WHERE time IN CAST(['2024-06-15 12:00:00'], 'Array(DateTime)');

SELECT 'test 13: affected partitions (should be all)';
SELECT arraySort(block_numbers.partition_id) as partitions
FROM system.mutations
WHERE database = currentDatabase() AND table = 't_mut_prune_13' AND NOT is_killed
ORDER BY mutation_id;

-- The same cast with the timezone pinned in the target type prunes, which shows test 13
-- turns on the missing timezone and not on the cast
DROP TABLE IF EXISTS t_mut_prune_13b;
CREATE TABLE t_mut_prune_13b (key UInt64, time DateTime)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mut_prune_13b', '1')
PARTITION BY toYYYYMM(time) ORDER BY key;

INSERT INTO t_mut_prune_13b SELECT number * 10 + 1, toDateTime(concat('2024-', lpad(toString(number * 5 + 1), 2, '0'), '-15 12:00:00')) FROM numbers(3);
INSERT INTO t_mut_prune_13b SELECT number * 10 + 9, toDateTime(concat('2024-', lpad(toString(number * 5 + 1), 2, '0'), '-15 12:00:00'), 'UTC') FROM numbers(3);

ALTER TABLE t_mut_prune_13b DELETE WHERE time IN CAST(['2024-06-15 12:00:00'], 'Array(DateTime(\'UTC\'))');

SELECT 'test 13b: affected partitions';
SELECT arraySort(block_numbers.partition_id) as partitions
FROM system.mutations
WHERE database = currentDatabase() AND table = 't_mut_prune_13b' AND NOT is_killed
ORDER BY mutation_id;

DROP TABLE t_mut_prune_13 SYNC;
DROP TABLE t_mut_prune_13b SYNC;
