-- `extremes` are computed by `ExtremesStep`, which is placed above the final `LimitStep`,
-- and the `SortingStep` feeding it already carries the same limit. Pushing that limit into
-- the FINAL merge therefore does not change which rows reach `ExtremesStep`, so the reported
-- extremes must be identical with and without `optimize_final_limit_pushdown`.

DROP TABLE IF EXISTS t_final_limit_extremes;

CREATE TABLE t_final_limit_extremes (key UInt64, v SimpleAggregateFunction(sum, UInt64))
ENGINE = AggregatingMergeTree ORDER BY key PARTITION BY key % 4;

INSERT INTO t_final_limit_extremes SELECT number, number FROM numbers(100);
INSERT INTO t_final_limit_extremes SELECT number, number FROM numbers(100);

SELECT 'ascending, pushdown off';
SELECT key FROM t_final_limit_extremes FINAL ORDER BY key LIMIT 5 SETTINGS extremes = 1, optimize_final_limit_pushdown = 0;

SELECT 'ascending, pushdown on';
SELECT key FROM t_final_limit_extremes FINAL ORDER BY key LIMIT 5 SETTINGS extremes = 1, optimize_final_limit_pushdown = 1;

SELECT 'ascending, pushdown on, sequential partitions';
SELECT key FROM t_final_limit_extremes FINAL ORDER BY key LIMIT 5
SETTINGS extremes = 1, optimize_final_limit_pushdown = 1, optimize_final_sequential_partitions = 1;

SELECT 'descending, pushdown off';
SELECT key FROM t_final_limit_extremes FINAL ORDER BY key DESC LIMIT 5 SETTINGS extremes = 1, optimize_final_limit_pushdown = 0;

SELECT 'descending, pushdown on';
SELECT key FROM t_final_limit_extremes FINAL ORDER BY key DESC LIMIT 5 SETTINGS extremes = 1, optimize_final_limit_pushdown = 1;

SELECT 'offset, pushdown off';
SELECT key FROM t_final_limit_extremes FINAL ORDER BY key LIMIT 3 OFFSET 4 SETTINGS extremes = 1, optimize_final_limit_pushdown = 0;

SELECT 'offset, pushdown on';
SELECT key FROM t_final_limit_extremes FINAL ORDER BY key LIMIT 3 OFFSET 4 SETTINGS extremes = 1, optimize_final_limit_pushdown = 1;

DROP TABLE t_final_limit_extremes;
