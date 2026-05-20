-- `WITH CLUSTER` cannot be combined with `WITH ROLLUP` / `WITH CUBE` /
-- `GROUPING SETS` / `WITH TOTALS`: the latter finalize aggregate states
-- before the cluster step would merge them, which would otherwise produce
-- a `LOGICAL_ERROR` in `mergeAggregateStates` ("Expected ColumnAggregateFunction").

SELECT count() FROM (SELECT toUInt64(number) AS x FROM numbers(4))
GROUP BY x WITH CLUSTER 1 WITH ROLLUP; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM (SELECT toUInt64(number) AS x FROM numbers(4))
GROUP BY x WITH CLUSTER 1 WITH CUBE; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM (SELECT toUInt64(number) AS x FROM numbers(4))
GROUP BY x WITH CLUSTER 1 WITH TOTALS; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM (SELECT toUInt64(number) AS x, toUInt64(number) AS y FROM numbers(4))
GROUP BY GROUPING SETS ((x WITH CLUSTER 1), (y)); -- { serverError BAD_ARGUMENTS, SYNTAX_ERROR }
