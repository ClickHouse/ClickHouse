-- Random settings limits: parallel_replicas_local_plan=(1, None); optimize_aggregation_in_order=(0, 0)

-- The two projection-use assertions read the initiator's own plan. With parallel replicas enabled
-- (the ParallelReplicas job enables them for every query) that plan drops the implicit projection
-- unless the initiator builds a local plan and aggregation-in-order is off, so the clamp above pins
-- both settings to the values they already have by default.

-- Part min-max index slots are addressed by position, so their order must not follow the table column
-- order: ALTER TABLE ... MODIFY COLUMN ... AFTER on a partition-key column used to desynchronize the
-- two and silently prune correct parts, no-op a DELETE, and misreport min_time / max_time.

DROP TABLE IF EXISTS t_minmax_order_prune;
DROP TABLE IF EXISTS t_minmax_order_time;
DROP TABLE IF EXISTS t_minmax_order_enum;
DROP TABLE IF EXISTS t_minmax_order_del_light;
DROP TABLE IF EXISTS t_minmax_order_del_heavy;
DROP TABLE IF EXISTS t_minmax_order_merge;
DROP TABLE IF EXISTS t_minmax_order_projection;
DROP TABLE IF EXISTS t_minmax_order_unsorted;

-- T1: a part written BEFORE the reorder must still be selected by the partition key.
CREATE TABLE t_minmax_order_prune (a UInt32, b UInt32) ENGINE = MergeTree PARTITION BY (a, b) ORDER BY tuple();
INSERT INTO t_minmax_order_prune VALUES (1, 100);
ALTER TABLE t_minmax_order_prune MODIFY COLUMN a UInt32 AFTER b;

SELECT 'T1 old part, a', count() FROM t_minmax_order_prune WHERE a = 1
SETTINGS use_partition_pruning = 1, use_skip_indexes = 1, optimize_use_projections = 0, optimize_use_implicit_projections = 0;
SELECT 'T1 old part, b', count() FROM t_minmax_order_prune WHERE b = 100
SETTINGS use_partition_pruning = 1, use_skip_indexes = 1, optimize_use_projections = 0, optimize_use_implicit_projections = 0;

-- T2: parts written before and after the reorder must both be selected by the same predicate.
INSERT INTO t_minmax_order_prune (a, b) VALUES (2, 200);

SELECT 'T2 both parts, a', count() FROM t_minmax_order_prune WHERE a IN (1, 2)
SETTINGS use_partition_pruning = 1, use_skip_indexes = 1, optimize_use_projections = 0, optimize_use_implicit_projections = 0;
SELECT 'T2 both parts, b', count() FROM t_minmax_order_prune WHERE b IN (100, 200)
SETTINGS use_partition_pruning = 1, use_skip_indexes = 1, optimize_use_projections = 0, optimize_use_implicit_projections = 0;
SELECT 'T2 new part, a', count() FROM t_minmax_order_prune WHERE a = 2
SETTINGS use_partition_pruning = 1, use_skip_indexes = 1, optimize_use_projections = 0, optimize_use_implicit_projections = 0;

-- T3: system.parts min_time / max_time must report the DateTime partition-key column, not its neighbour.
CREATE TABLE t_minmax_order_time (d DateTime, x UInt32) ENGINE = MergeTree PARTITION BY (d, x) ORDER BY tuple();
INSERT INTO t_minmax_order_time VALUES ('2020-01-01 00:00:00', 7);
ALTER TABLE t_minmax_order_time MODIFY COLUMN d DateTime AFTER x;
INSERT INTO t_minmax_order_time (d, x) VALUES ('2021-06-06 06:06:06', 8);

SELECT 'T3 min_time/max_time', min_time, max_time FROM system.parts
WHERE database = currentDatabase() AND table = 't_minmax_order_time' AND active ORDER BY min_time;

-- T4: reading min_time for a reordered Nullable/DateTime partition key must not throw a
-- LOGICAL_ERROR, and must report c2's own range: a non-epoch value is used so the expected output
-- is not also reachable from the empty-range sentinel.
CREATE TABLE t_minmax_order_enum (c1 Enum('a' = 1) NULL, c2 DateTime) ENGINE = MergeTree
PARTITION BY (c1, c2) ORDER BY tuple() SETTINGS allow_nullable_key = 1;
ALTER TABLE t_minmax_order_enum MODIFY COLUMN c1 Nullable(Int8) AFTER c2;
INSERT INTO TABLE t_minmax_order_enum (c1, c2) VALUES (1, '2024-06-15 12:00:00');

SELECT 'T4 min_time/max_time', min_time, max_time FROM system.parts
WHERE database = currentDatabase() AND table = 't_minmax_order_enum' AND active;

-- T5: a lightweight DELETE must not silently keep the rows it selected.
CREATE TABLE t_minmax_order_del_light (a UInt32, b UInt32) ENGINE = MergeTree PARTITION BY (a, b) ORDER BY tuple();
INSERT INTO t_minmax_order_del_light VALUES (1, 100);
ALTER TABLE t_minmax_order_del_light MODIFY COLUMN a UInt32 AFTER b;
DELETE FROM t_minmax_order_del_light WHERE a = 1 SETTINGS lightweight_deletes_sync = 2;

SELECT 'T5 rows after lightweight DELETE', count() FROM t_minmax_order_del_light
SETTINGS use_partition_pruning = 1, use_skip_indexes = 1, optimize_use_projections = 0, optimize_use_implicit_projections = 0;

-- T5b: neither must a heavyweight ALTER ... DELETE.
CREATE TABLE t_minmax_order_del_heavy (a UInt32, b UInt32) ENGINE = MergeTree PARTITION BY (a, b) ORDER BY tuple();
INSERT INTO t_minmax_order_del_heavy VALUES (1, 100);
ALTER TABLE t_minmax_order_del_heavy MODIFY COLUMN a UInt32 AFTER b;
ALTER TABLE t_minmax_order_del_heavy DELETE WHERE a = 1 SETTINGS mutations_sync = 2;

SELECT 'T5b rows after ALTER DELETE', count() FROM t_minmax_order_del_heavy
SETTINGS use_partition_pruning = 1, use_skip_indexes = 1, optimize_use_projections = 0, optimize_use_implicit_projections = 0;

-- T6: a merge combines the source min-max indices by position and stores the result by name, so a
-- desynchronized order is written to disk and survives a reload.
CREATE TABLE t_minmax_order_merge (a UInt32, b UInt32) ENGINE = MergeTree PARTITION BY (a, intDiv(b, 100)) ORDER BY tuple();
INSERT INTO t_minmax_order_merge VALUES (1000, 100);
ALTER TABLE t_minmax_order_merge MODIFY COLUMN a UInt32 AFTER b;
INSERT INTO t_minmax_order_merge (a, b) VALUES (1000, 150);
OPTIMIZE TABLE t_minmax_order_merge FINAL;

SELECT 'T6 after merge', count() FROM t_minmax_order_merge WHERE b = 100
SETTINGS use_partition_pruning = 1, use_skip_indexes = 1, optimize_use_projections = 0, optimize_use_implicit_projections = 0;

DETACH TABLE t_minmax_order_merge;
ATTACH TABLE t_minmax_order_merge;

SELECT 'T6 after reload', count() FROM t_minmax_order_merge WHERE b = 100
SETTINGS use_partition_pruning = 1, use_skip_indexes = 1, optimize_use_projections = 0, optimize_use_implicit_projections = 0;

-- T7: the implicit _minmax_count_projection pairs its i-th min/max with slot i of the same index, so
-- min() / max() answered from it must agree with the answer that reads the parts.
CREATE TABLE t_minmax_order_projection (a UInt32, b UInt32) ENGINE = MergeTree PARTITION BY (a, b) ORDER BY tuple();
INSERT INTO t_minmax_order_projection VALUES (1, 100);
ALTER TABLE t_minmax_order_projection MODIFY COLUMN a UInt32 AFTER b;
INSERT INTO t_minmax_order_projection (a, b) VALUES (2, 200);

SELECT 'T7 projection enabled', min(a), max(a), min(b), max(b) FROM t_minmax_order_projection
SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SELECT 'T7 projection disabled', min(a), max(a), min(b), max(b) FROM t_minmax_order_projection
SETTINGS optimize_use_projections = 0, optimize_use_implicit_projections = 0;
SELECT 'T7 projection used', count() > 0 FROM (
    EXPLAIN SELECT min(a), max(a), min(b), max(b) FROM t_minmax_order_projection
    SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1
) WHERE explain ILIKE '%_minmax_count_projection%';

-- T8: guards the mechanism itself. No ALTER at all, but the table column order is not the sorted
-- order, so the index slot order and the projection pair order must still be derived the same way.
CREATE TABLE t_minmax_order_unsorted (b UInt32, a UInt32) ENGINE = MergeTree PARTITION BY (a, intDiv(b, 100)) ORDER BY tuple();
INSERT INTO t_minmax_order_unsorted VALUES (100, 1);
INSERT INTO t_minmax_order_unsorted VALUES (250, 1);

SELECT 'T8 projection enabled', min(a), max(a), min(b), max(b) FROM t_minmax_order_unsorted
SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SELECT 'T8 projection disabled', min(a), max(a), min(b), max(b) FROM t_minmax_order_unsorted
SETTINGS optimize_use_projections = 0, optimize_use_implicit_projections = 0;
SELECT 'T8 projection used', count() > 0 FROM (
    EXPLAIN SELECT min(a), max(a), min(b), max(b) FROM t_minmax_order_unsorted
    SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1
) WHERE explain ILIKE '%_minmax_count_projection%';
SELECT 'T8 pruning, b', count() FROM t_minmax_order_unsorted WHERE b = 250
SETTINGS use_partition_pruning = 1, use_skip_indexes = 1, optimize_use_projections = 0, optimize_use_implicit_projections = 0;

DROP TABLE t_minmax_order_prune;
DROP TABLE t_minmax_order_time;
DROP TABLE t_minmax_order_enum;
DROP TABLE t_minmax_order_del_light;
DROP TABLE t_minmax_order_del_heavy;
DROP TABLE t_minmax_order_merge;
DROP TABLE t_minmax_order_projection;
DROP TABLE t_minmax_order_unsorted;
