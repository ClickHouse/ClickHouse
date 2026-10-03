-- CLEAR COLUMN of a source of a MATERIALIZED key column keeps the key column's stored value,
-- while non-key MATERIALIZED dependents of the cleared column are still recalculated.

-- Sorting key: recalculated values would be out of order.
DROP TABLE IF EXISTS t_order;
CREATE TABLE t_order (x Int32, y Int32, mx Int32 MATERIALIZED x + y)
ENGINE = MergeTree ORDER BY mx SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1;
INSERT INTO t_order (x, y) VALUES (100, 1), (1, 50), (2, 60), (200, 3);
ALTER TABLE t_order CLEAR COLUMN x SETTINGS mutations_sync = 2;
SELECT 'order', groupArray((x, y, mx)) FROM (SELECT x, y, mx FROM t_order ORDER BY _part_offset);
OPTIMIZE TABLE t_order FINAL;
SELECT 'order after optimize', groupArray((x, y, mx)) FROM (SELECT x, y, mx FROM t_order ORDER BY _part_offset);

-- Sorting key: recalculated values would stay sorted, so only the primary index would disagree with the data.
DROP TABLE IF EXISTS t_index;
CREATE TABLE t_index (x Int32, y Int32, mx Int32 MATERIALIZED x + y)
ENGINE = MergeTree ORDER BY mx SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1;
INSERT INTO t_index (x, y) VALUES (100, 1), (200, 3), (300, 5);
ALTER TABLE t_index CLEAR COLUMN x SETTINGS mutations_sync = 2;
SELECT 'index', groupArray((x, y, mx)) FROM (SELECT x, y, mx FROM t_index ORDER BY _part_offset);
SELECT 'index WHERE mx = 203', count() FROM t_index WHERE mx = 203;
SELECT 'index countIf(mx = 203)', countIf(mx = 203) FROM t_index;

-- Partition key.
DROP TABLE IF EXISTS t_partition;
CREATE TABLE t_partition (x Int32, y Int32, p Int32 MATERIALIZED x)
ENGINE = MergeTree PARTITION BY p ORDER BY y SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_partition (x, y) VALUES (5, 1), (5, 2);
ALTER TABLE t_partition CLEAR COLUMN x SETTINGS mutations_sync = 2;
SELECT 'partition parts', groupArray(partition) FROM system.parts WHERE database = currentDatabase() AND table = 't_partition' AND active;
SELECT 'partition', groupArray((x, y, p)) FROM (SELECT x, y, p FROM t_partition ORDER BY y);
SELECT 'partition WHERE p = 5', count() FROM t_partition WHERE p = 5;

-- Sorting key on a subcolumn of a MATERIALIZED column.
DROP TABLE IF EXISTS t_subcolumn;
CREATE TABLE t_subcolumn (x Int32, y Int32, m Tuple(a Int32) MATERIALIZED tuple(x + y))
ENGINE = MergeTree ORDER BY m.a SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1;
INSERT INTO t_subcolumn (x, y) VALUES (100, 1), (200, 3), (300, 5);
ALTER TABLE t_subcolumn CLEAR COLUMN x SETTINGS mutations_sync = 2;
SELECT 'subcolumn', groupArray((x, y, m.a)) FROM (SELECT x, y, m FROM t_subcolumn ORDER BY _part_offset);
SELECT 'subcolumn WHERE m.a = 203', count() FROM t_subcolumn WHERE m.a = 203;
SELECT 'subcolumn countIf(m.a = 203)', countIf(m.a = 203) FROM t_subcolumn;

-- Key reached through a non-key MATERIALIZED column, with dependents on both sides of it.
DROP TABLE IF EXISTS t_chain;
CREATE TABLE t_chain
(
    x Int32,
    y Int32,
    i Int32 MATERIALIZED x * 2,
    k Int32 MATERIALIZED i + y,
    w Int32 MATERIALIZED k * 10,
    v Int32 MATERIALIZED x + k,
    z Int32 MATERIALIZED x + 1
)
ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1;
INSERT INTO t_chain (x, y) VALUES (100, 1), (200, 3);
ALTER TABLE t_chain CLEAR COLUMN x SETTINGS mutations_sync = 2;
SELECT 'chain', groupArray((x, y, i, k, w, v, z)) FROM (SELECT x, y, i, k, w, v, z FROM t_chain ORDER BY _part_offset);
SELECT 'chain WHERE k = 201', count() FROM t_chain WHERE k = 201;
SELECT 'chain countIf(k = 201)', countIf(k = 201) FROM t_chain;

-- Version column of ReplacingMergeTree.
DROP TABLE IF EXISTS t_version;
CREATE TABLE t_version (id Int32, x Int32, v UInt32 MATERIALIZED toUInt32(x))
ENGINE = ReplacingMergeTree(v) ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_version (id, x) VALUES (1, 10);
ALTER TABLE t_version CLEAR COLUMN x SETTINGS mutations_sync = 2;
SELECT 'version', groupArray((id, x, v)) FROM t_version;

SELECT 'unfinished mutations', count() FROM system.mutations WHERE database = currentDatabase() AND NOT is_done;
SELECT 'finished mutations', count() FROM system.mutations WHERE database = currentDatabase() AND is_done;

DROP TABLE t_order;
DROP TABLE t_index;
DROP TABLE t_partition;
DROP TABLE t_subcolumn;
DROP TABLE t_chain;
DROP TABLE t_version;
