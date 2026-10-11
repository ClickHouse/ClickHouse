-- The partition value index selects the partitions that can match the filter before the parts are checked one by one.
-- It is shown in EXPLAIN as a separate index, before the min-max index and the partition key.
-- Only the `Indexes` section of EXPLAIN is printed, the rest of the plan is not relevant here.

DROP TABLE IF EXISTS t_partition_value_index;
DROP TABLE IF EXISTS t_partition_value_index_string;
DROP TABLE IF EXISTS t_partition_value_index_bool;

-- 30 partitions (10 tenants, 3 days) with one part each.
CREATE TABLE t_partition_value_index (tenant UInt16, ts DateTime('UTC'), v UInt64)
ENGINE = MergeTree PARTITION BY (tenant, toYYYYMMDD(ts)) ORDER BY v;

INSERT INTO t_partition_value_index
SELECT number % 10, toDateTime('2026-01-01 00:00:00', 'UTC') + (number % 3) * 86400, number
FROM numbers(30) SETTINGS max_partitions_per_insert_block = 0;

SET partition_value_index_min_parts = 0;
-- With parallel replicas, the plan does not show the indexes of the read.
SET enable_parallel_replicas = 0;
-- The result of the generic exclusion search depends on the granularity, and the conditions shown on the preimage optimization.
SET merge_tree_coarse_index_granularity = 8;
SET optimize_time_filter_with_preimage = 1;

SELECT 'Point on the first partition key column, binary search';
SELECT trimLeft(line) FROM (SELECT arrayJoin(arraySlice(groupArray(explain), arrayFirstIndex(x -> trimLeft(x) = 'Indexes:', groupArray(explain)))) AS line
FROM (EXPLAIN indexes = 1 SELECT sum(v) FROM t_partition_value_index WHERE tenant = 7));
SELECT sum(v) FROM t_partition_value_index WHERE tenant = 7;

SELECT 'Point and range, binary search';
SELECT trimLeft(line) FROM (SELECT arrayJoin(arraySlice(groupArray(explain), arrayFirstIndex(x -> trimLeft(x) = 'Indexes:', groupArray(explain)))) AS line
FROM (EXPLAIN indexes = 1 SELECT sum(v) FROM t_partition_value_index WHERE tenant = 7 AND ts >= toDateTime('2026-01-02 00:00:00', 'UTC')));
SELECT sum(v) FROM t_partition_value_index WHERE tenant = 7 AND ts >= toDateTime('2026-01-02 00:00:00', 'UTC');

SELECT 'Set, generic exclusion search';
SELECT trimLeft(line) FROM (SELECT arrayJoin(arraySlice(groupArray(explain), arrayFirstIndex(x -> trimLeft(x) = 'Indexes:', groupArray(explain)))) AS line
FROM (EXPLAIN indexes = 1 SELECT sum(v) FROM t_partition_value_index WHERE tenant IN (1, 5, 9)));
SELECT sum(v) FROM t_partition_value_index WHERE tenant IN (1, 5, 9);

SELECT 'Second partition key column only, generic exclusion search';
SELECT trimLeft(line) FROM (SELECT arrayJoin(arraySlice(groupArray(explain), arrayFirstIndex(x -> trimLeft(x) = 'Indexes:', groupArray(explain)))) AS line
FROM (EXPLAIN indexes = 1 SELECT sum(v) FROM t_partition_value_index WHERE toYYYYMMDD(ts) = 20260102));
SELECT sum(v) FROM t_partition_value_index WHERE toYYYYMMDD(ts) = 20260102;

-- With a tiny step budget, the search stops at its step limit and selects the remaining partitions as a whole,
-- which are then checked part by part.
SELECT 'Second partition key column only, generic exclusion search reaches the step limit';
SELECT trimLeft(line) FROM (SELECT arrayJoin(arraySlice(groupArray(explain), arrayFirstIndex(x -> trimLeft(x) = 'Indexes:', groupArray(explain)))) AS line
FROM (EXPLAIN indexes = 1 SELECT sum(v) FROM t_partition_value_index WHERE toYYYYMMDD(ts) = 20260102 SETTINGS partition_value_index_max_steps = 1));
SELECT sum(v) FROM t_partition_value_index WHERE toYYYYMMDD(ts) = 20260102 SETTINGS partition_value_index_max_steps = 1;

SELECT 'Condition that does not match any partition';
SELECT trimLeft(line) FROM (SELECT arrayJoin(arraySlice(groupArray(explain), arrayFirstIndex(x -> trimLeft(x) = 'Indexes:', groupArray(explain)))) AS line
FROM (EXPLAIN indexes = 1 SELECT sum(v) FROM t_partition_value_index WHERE tenant = 1000));

SELECT 'The index is not used for a condition it cannot analyze';
SELECT countIf(explain LIKE '%PartitionValueIndex%') FROM (EXPLAIN indexes = 1 SELECT sum(v) FROM t_partition_value_index WHERE cityHash64(tenant) % 4 = 1);

SELECT 'The index is not used for tables with fewer parts than partition_value_index_min_parts';
SELECT countIf(explain LIKE '%PartitionValueIndex%') FROM (EXPLAIN indexes = 1 SELECT sum(v) FROM t_partition_value_index WHERE tenant = 7 SETTINGS partition_value_index_min_parts = 1000);

SELECT 'The index is not used if disabled';
SELECT countIf(explain LIKE '%PartitionValueIndex%') FROM (EXPLAIN indexes = 1 SELECT sum(v) FROM t_partition_value_index WHERE tenant = 7 SETTINGS use_partition_value_index = 0);

SELECT 'String partition key, with partition IDs that are hashes';
CREATE TABLE t_partition_value_index_string (tenant String, v UInt64) ENGINE = MergeTree PARTITION BY tenant ORDER BY v;
INSERT INTO t_partition_value_index_string SELECT concat('tenant_', toString(number % 20)), number FROM numbers(40) SETTINGS max_partitions_per_insert_block = 0;
SELECT trimLeft(line) FROM (SELECT arrayJoin(arraySlice(groupArray(explain), arrayFirstIndex(x -> trimLeft(x) = 'Indexes:', groupArray(explain)))) AS line
FROM (EXPLAIN indexes = 1 SELECT sum(v) FROM t_partition_value_index_string WHERE tenant = 'tenant_7'));
SELECT sum(v) FROM t_partition_value_index_string WHERE tenant = 'tenant_7';

SELECT 'Bool partition key';
CREATE TABLE t_partition_value_index_bool (b Bool, v UInt64) ENGINE = MergeTree PARTITION BY b ORDER BY v;
INSERT INTO t_partition_value_index_bool SELECT number % 2, number FROM numbers(10);
SELECT countIf(explain LIKE '%PartitionValueIndex%') FROM (EXPLAIN indexes = 1 SELECT sum(v) FROM t_partition_value_index_bool WHERE b);
SELECT sum(v) FROM t_partition_value_index_bool WHERE b;

DROP TABLE t_partition_value_index;
DROP TABLE t_partition_value_index_string;
DROP TABLE t_partition_value_index_bool;
