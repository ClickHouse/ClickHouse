SET enable_analyzer = 1;
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
SET optimize_use_projections = 1, optimize_read_in_order = 1;

DROP TABLE IF EXISTS t_errors;
DROP TABLE IF EXISTS t_errors_replacing;
DROP TABLE IF EXISTS t_errors_memory;
DROP TABLE IF EXISTS t_errors_distributed;

CREATE TABLE t_errors
(
    key UInt64,
    value UInt64,
    other UInt64,
    PROJECTION p_other (SELECT key, value, other ORDER BY other),
    PROJECTION p_value (SELECT key, value ORDER BY value)
)
ENGINE = MergeTree ORDER BY key;

INSERT INTO t_errors SELECT number, number * 2, number % 100 FROM numbers(1000);

CREATE TABLE t_errors_replacing
(
    key UInt64,
    value UInt64,
    PROJECTION p_value (SELECT key, value ORDER BY value)
)
ENGINE = ReplacingMergeTree ORDER BY key
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

INSERT INTO t_errors_replacing SELECT number, number FROM numbers(100);

CREATE TABLE t_errors_memory (key UInt64) ENGINE = Memory;
CREATE TABLE t_errors_distributed AS t_errors ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_errors);

-- Rejected by the analyzer.
SELECT value FROM t_errors PROJECTION p_missing WHERE key < 10; -- { serverError NO_SUCH_PROJECTION_IN_TABLE }
SELECT key FROM t_errors_memory PROJECTION p_value; -- { serverError ILLEGAL_PROJECTION }
SELECT value FROM t_errors_distributed PROJECTION p_other WHERE key < 10; -- { serverError ILLEGAL_PROJECTION }
SELECT value FROM t_errors_replacing FINAL PROJECTION p_value WHERE value < 10; -- { serverError ILLEGAL_PROJECTION }
SELECT value FROM t_errors PROJECTION p_other WHERE key < 10 SETTINGS optimize_use_projections = 0; -- { serverError SUPPORT_IS_DISABLED }
SELECT value FROM t_errors PROJECTION p_other WHERE key < 10 SETTINGS make_distributed_plan = 1; -- { serverError SUPPORT_IS_DISABLED }

-- Rejected by the optimizer: the projection cannot serve the read.
SELECT other FROM t_errors PROJECTION p_value WHERE value < 10; -- { serverError PROJECTION_NOT_USED }
SELECT key FROM t_errors PROJECTION p_other WHERE key < 10 ORDER BY key; -- { serverError PROJECTION_NOT_USED }

ALTER TABLE t_errors ADD PROJECTION p_late (SELECT key, value ORDER BY value);
SELECT value FROM t_errors PROJECTION p_late WHERE value < 10; -- { serverError PROJECTION_NOT_USED }
