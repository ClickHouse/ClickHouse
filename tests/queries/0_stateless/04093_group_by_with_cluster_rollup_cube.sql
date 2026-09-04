-- `WITH CLUSTER` cannot be combined with `WITH ROLLUP` / `WITH CUBE` /
-- `GROUPING SETS` / `WITH TOTALS`: the latter finalize aggregate states
-- before the cluster step would merge them, which would otherwise produce
-- a `LOGICAL_ERROR` in `mergeAggregateStates` ("Expected ColumnAggregateFunction").
-- `GROUPING SETS` has its own element grammar that does not accept the
-- `WITH CLUSTER` modifier, so that combination is already rejected by the parser.

SET allow_experimental_group_by_with_cluster = 1;

SELECT count() FROM (SELECT toUInt64(number) AS x FROM numbers(4))
GROUP BY x WITH CLUSTER 1 WITH ROLLUP; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM (SELECT toUInt64(number) AS x FROM numbers(4))
GROUP BY x WITH CLUSTER 1 WITH CUBE; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM (SELECT toUInt64(number) AS x FROM numbers(4))
GROUP BY x WITH CLUSTER 1 WITH TOTALS; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM (SELECT toUInt64(number) AS x, toUInt64(number % 2) AS y FROM numbers(4))
GROUP BY GROUPING SETS ((x WITH CLUSTER 1), (y)); -- { clientError SYNTAX_ERROR }

SELECT count() FROM (SELECT toUInt64(number) AS x, toUInt64(number % 2) AS y FROM numbers(4))
GROUP BY GROUPING SETS ((x), (y)) WITH CLUSTER 1; -- { clientError SYNTAX_ERROR }
