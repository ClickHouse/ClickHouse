-- A normal projection rejected after the table read was narrowed to the parts the projection does not cover
-- must leave the table read with all its parts.

DROP TABLE IF EXISTS t_commit_order;
CREATE TABLE t_commit_order
(
    a UInt64,
    PROJECTION p INDEX * TYPE commit_order WITH SETTINGS (index_granularity = 4, index_granularity_bytes = '10Mi')
)
ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1,
    index_granularity = 1, index_granularity_bytes = '10Mi';

INSERT INTO t_commit_order SELECT number FROM numbers(16);
INSERT INTO t_commit_order SELECT number FROM numbers(16);
-- A commit-order projection is built only by merges.
OPTIMIZE TABLE t_commit_order FINAL;
SYSTEM STOP MERGES t_commit_order;
INSERT INTO t_commit_order SELECT number FROM numbers(16);

SELECT count() FROM (SELECT a FROM t_commit_order WHERE _block_number <= 255)
SETTINGS optimize_use_projections = 1, query_plan_remove_unused_columns = 0, log_comment = 'normal_projection_rejected_keeps_parent_parts';

SYSTEM FLUSH LOGS query_log;
SELECT projections FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = 'normal_projection_rejected_keeps_parent_parts' AND type = 'QueryFinish';

DROP TABLE t_commit_order;

DROP TABLE IF EXISTS t_normal;
CREATE TABLE t_normal (i Int32) ENGINE = MergeTree ORDER BY tuple();
SYSTEM STOP MERGES t_normal;
INSERT INTO t_normal VALUES (1);
ALTER TABLE t_normal ADD PROJECTION x (SELECT i ORDER BY i);
INSERT INTO t_normal VALUES (2);

SELECT 1 FROM t_normal WHERE materialize(1)
SETTINGS optimize_use_projections = 1, prefer_optimize_projection = 1, query_plan_remove_unused_columns = 0;

DROP TABLE t_normal;

-- `compatibility` up to 25.11 sets `query_plan_remove_unused_columns = 0`.
DROP TABLE IF EXISTS t_partial;
CREATE TABLE t_partial (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES t_partial;
INSERT INTO t_partial SELECT number, number % 10 FROM numbers(100);
ALTER TABLE t_partial ADD PROJECTION p (SELECT * ORDER BY b);
INSERT INTO t_partial SELECT number, number % 10 FROM numbers(100);

SELECT count() FROM (SELECT a FROM t_partial WHERE b = 5)
SETTINGS optimize_use_projections = 1, query_plan_remove_unused_columns = 0;

DROP TABLE t_partial;
