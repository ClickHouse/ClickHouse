-- `WITH CLUSTER` is only implemented for the new analyzer; reject it
-- early on the legacy (`enable_analyzer = 0`) path with `BAD_ARGUMENTS`.

SET enable_analyzer = 0;

SELECT count() FROM (SELECT toUInt64(number) AS x FROM numbers(4))
GROUP BY x WITH CLUSTER 1; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM (
    SELECT toUInt64(number) AS x, toUInt64(number) AS y FROM numbers(4)
)
GROUP BY (x, y) WITH CLUSTER 1; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM (SELECT 'abc' AS s)
GROUP BY s WITH CLUSTER 1; -- { serverError BAD_ARGUMENTS }
