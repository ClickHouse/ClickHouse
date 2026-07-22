-- Tags: no-random-merge-tree-settings

SET mutations_sync = 2;
SET alter_sync = 2;
SET optimize_on_insert = 0;

DROP TABLE IF EXISTS t_summing_clear_required;
CREATE TABLE t_summing_clear_required
(
    k UInt32,
    v Int64
)
ENGINE = SummingMergeTree()
ORDER BY k
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_summing_clear_required VALUES (1, 5), (2, 10);
INSERT INTO t_summing_clear_required VALUES (2, -10);
ALTER TABLE t_summing_clear_required CLEAR COLUMN v;
OPTIMIZE TABLE t_summing_clear_required FINAL;

SELECT 'summing_clear_rows', count() FROM t_summing_clear_required;
SELECT 'summing_clear_parts_column_v', count()
FROM system.parts_columns
WHERE database = currentDatabase()
  AND table = 't_summing_clear_required'
  AND active
  AND column = 'v';

DROP TABLE t_summing_clear_required;

DROP TABLE IF EXISTS t_summing_clear_partition_compact;
CREATE TABLE t_summing_clear_partition_compact
(
    p UInt32,
    k UInt32,
    v Int64
)
ENGINE = SummingMergeTree()
PARTITION BY p
ORDER BY k;

SYSTEM STOP MERGES t_summing_clear_partition_compact;
INSERT INTO t_summing_clear_partition_compact VALUES (1, 1, 5), (1, 2, 10);
INSERT INTO t_summing_clear_partition_compact VALUES (1, 2, -10);
SYSTEM START MERGES t_summing_clear_partition_compact;
ALTER TABLE t_summing_clear_partition_compact CLEAR COLUMN v IN PARTITION 1;
OPTIMIZE TABLE t_summing_clear_partition_compact PARTITION 1 FINAL;

SELECT 'summing_clear_partition_compact_rows', count() FROM t_summing_clear_partition_compact;

DROP TABLE t_summing_clear_partition_compact;

DROP TABLE IF EXISTS t_summing_update_zero_parity;
CREATE TABLE t_summing_update_zero_parity
(
    p UInt32,
    k UInt32,
    v Int64
)
ENGINE = SummingMergeTree()
PARTITION BY p
ORDER BY k;

SYSTEM STOP MERGES t_summing_update_zero_parity;
INSERT INTO t_summing_update_zero_parity VALUES (1, 1, 5), (1, 2, 10);
INSERT INTO t_summing_update_zero_parity VALUES (1, 2, -10);
SYSTEM START MERGES t_summing_update_zero_parity;
ALTER TABLE t_summing_update_zero_parity UPDATE v = 0 IN PARTITION 1 WHERE 1;
OPTIMIZE TABLE t_summing_update_zero_parity PARTITION 1 FINAL;

SELECT 'summing_update_zero_parity_rows', count() FROM t_summing_update_zero_parity;

DROP TABLE t_summing_update_zero_parity;

DROP TABLE IF EXISTS t_summing_add_required;
CREATE TABLE t_summing_add_required
(
    k UInt32
)
ENGINE = SummingMergeTree()
ORDER BY k
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_summing_add_required;
INSERT INTO t_summing_add_required VALUES (1), (2);
INSERT INTO t_summing_add_required VALUES (1);
ALTER TABLE t_summing_add_required ADD COLUMN v UInt64;
SYSTEM START MERGES t_summing_add_required;
OPTIMIZE TABLE t_summing_add_required FINAL;

SELECT 'summing_add_column_rows', count() FROM t_summing_add_required;
SELECT 'summing_add_column_parts_column_v', count()
FROM system.parts_columns
WHERE database = currentDatabase()
  AND table = 't_summing_add_required'
  AND active
  AND column = 'v';

DROP TABLE t_summing_add_required;

DROP TABLE IF EXISTS t_summing_ttl_required;
CREATE TABLE t_summing_ttl_required
(
    d Date,
    k UInt32,
    expired UInt64 TTL d + INTERVAL 1 DAY,
    keep UInt64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(d)
ORDER BY k
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_summing_ttl_required;
INSERT INTO t_summing_ttl_required VALUES ('2001-09-09', 1, 7, 10);
INSERT INTO t_summing_ttl_required VALUES ('2001-09-09', 1, 11, 20);
SYSTEM START MERGES t_summing_ttl_required;
OPTIMIZE TABLE t_summing_ttl_required PARTITION 200109 FINAL;
OPTIMIZE TABLE t_summing_ttl_required PARTITION 200109 FINAL;

SELECT 'summing_ttl_values', k, expired, keep FROM t_summing_ttl_required ORDER BY k;
SELECT 'summing_ttl_parts_column_expired', count()
FROM system.parts_columns
WHERE database = currentDatabase()
  AND table = 't_summing_ttl_required'
  AND active
  AND column = 'expired';

DROP TABLE t_summing_ttl_required;

DROP TABLE IF EXISTS t_coalescing_clear_required;
CREATE TABLE t_coalescing_clear_required
(
    k UInt32,
    v UInt64
)
ENGINE = CoalescingMergeTree()
ORDER BY k
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_coalescing_clear_required VALUES (1, 10);
INSERT INTO t_coalescing_clear_required VALUES (1, 20);
ALTER TABLE t_coalescing_clear_required CLEAR COLUMN v;
OPTIMIZE TABLE t_coalescing_clear_required FINAL;

SELECT 'coalescing_clear_values', k, v FROM t_coalescing_clear_required ORDER BY k;
SELECT 'coalescing_clear_parts_column_v', count()
FROM system.parts_columns
WHERE database = currentDatabase()
  AND table = 't_coalescing_clear_required'
  AND active
  AND column = 'v';

DROP TABLE t_coalescing_clear_required;

DROP TABLE IF EXISTS t_aggregating_clear_required;
CREATE TABLE t_aggregating_clear_required
(
    k UInt32,
    s AggregateFunction(sum, UInt64),
    keep AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree()
ORDER BY k
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_aggregating_clear_required SELECT 1, sumState(toUInt64(7)), sumState(toUInt64(70)) FROM numbers(1);
INSERT INTO t_aggregating_clear_required SELECT 1, sumState(toUInt64(11)), sumState(toUInt64(110)) FROM numbers(1);
ALTER TABLE t_aggregating_clear_required CLEAR COLUMN s;

OPTIMIZE TABLE t_aggregating_clear_required FINAL;

SELECT 'aggregating_clear_state', k, finalizeAggregation(s), finalizeAggregation(keep) FROM t_aggregating_clear_required ORDER BY k;
SELECT 'aggregating_clear_parts_column_s', count()
FROM system.parts_columns
WHERE database = currentDatabase()
  AND table = 't_aggregating_clear_required'
  AND active
  AND column = 's';

DROP TABLE t_aggregating_clear_required;

DROP TABLE IF EXISTS t_aggregating_ttl_required;
CREATE TABLE t_aggregating_ttl_required
(
    d Date,
    k UInt32,
    expired SimpleAggregateFunction(sum, UInt64) TTL d + INTERVAL 1 DAY,
    keep SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(d)
ORDER BY k
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_aggregating_ttl_required;
INSERT INTO t_aggregating_ttl_required VALUES ('2001-09-09', 1, 7, 10);
INSERT INTO t_aggregating_ttl_required VALUES ('2001-09-09', 1, 11, 20);
SYSTEM START MERGES t_aggregating_ttl_required;
OPTIMIZE TABLE t_aggregating_ttl_required PARTITION 200109 FINAL;
OPTIMIZE TABLE t_aggregating_ttl_required PARTITION 200109 FINAL;

SELECT 'aggregating_ttl_values', k, expired, keep FROM t_aggregating_ttl_required ORDER BY k;
SELECT 'aggregating_ttl_parts_column_expired', count()
FROM system.parts_columns
WHERE database = currentDatabase()
  AND table = 't_aggregating_ttl_required'
  AND active
  AND column = 'expired';

DROP TABLE t_aggregating_ttl_required;

DROP TABLE IF EXISTS t_replacing_clear_regression;
CREATE TABLE t_replacing_clear_regression
(
    k UInt32,
    version UInt64,
    v UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY k
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_replacing_clear_regression VALUES (1, 1, 10);
INSERT INTO t_replacing_clear_regression VALUES (1, 2, 20);
ALTER TABLE t_replacing_clear_regression CLEAR COLUMN v;
OPTIMIZE TABLE t_replacing_clear_regression FINAL;

SELECT 'replacing_clear_regression', k, version, v FROM t_replacing_clear_regression ORDER BY k;
SELECT 'replacing_clear_parts_column_v', count()
FROM system.parts_columns
WHERE database = currentDatabase()
  AND table = 't_replacing_clear_regression'
  AND active
  AND column = 'v';

DROP TABLE t_replacing_clear_regression;
