-- `ALTER TABLE ... MODIFY PROJECTION` may only change the `WITH SETTINGS` clause, and the projection
-- body is compared as an AST: a restatement that differs only by comparison-insensitive spelling
-- (redundant parentheses, function name case) is the same body and must be accepted.

DROP TABLE IF EXISTS t_modify_projection_restatement;

CREATE TABLE t_modify_projection_restatement
(
    k UInt64,
    v UInt64,
    PROJECTION p (SELECT k, sum(v) GROUP BY k) WITH SETTINGS (index_granularity = 64)
)
ENGINE = MergeTree ORDER BY k;

-- Identical restatement, only the settings change.
ALTER TABLE t_modify_projection_restatement MODIFY PROJECTION p (SELECT k, sum(v) GROUP BY k) WITH SETTINGS (index_granularity = 128);

-- Redundant parentheses around an expression.
ALTER TABLE t_modify_projection_restatement MODIFY PROJECTION p (SELECT k, sum((v)) GROUP BY (k)) WITH SETTINGS (index_granularity = 256);

-- Function name spelled in a different case.
ALTER TABLE t_modify_projection_restatement MODIFY PROJECTION p (SELECT k, SUM(v) GROUP BY k) WITH SETTINGS (index_granularity = 512);

SELECT name, settings FROM system.projections
WHERE database = currentDatabase() AND table = 't_modify_projection_restatement';

-- A genuinely different body is still rejected.
ALTER TABLE t_modify_projection_restatement MODIFY PROJECTION p (SELECT k, max(v) GROUP BY k) WITH SETTINGS (index_granularity = 512); -- { serverError BAD_ARGUMENTS }

DROP TABLE t_modify_projection_restatement;
