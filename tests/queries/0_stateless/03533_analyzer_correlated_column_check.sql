SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

SELECT deltaSumMerge(rows) AS delta_sum FROM (SELECT * FROM (SELECT 1 AS x, deltaSumState(arrayJoin([3, 5])) AS rows UNION ALL SELECT 3, deltaSumState(arrayJoin([4, 6])) IGNORE NULLS AS rows WITH TOTALS UNION ALL SELECT DISTINCT 2, deltaSumState(*) IGNORE NULLS AS rows QUALIFY delta_sum = ignore(ignore(materialize(1023), *, *, toUInt256(10), *, *, 10, *, toUInt256(toUInt128(10)), 10 IS NOT NULL, 10, 10, 10, 10, materialize(toNullable(toUInt128(10))), *, isNullable(NULL), 10))) ORDER BY 1 DESC, ignore(*, *, 10, *, 10, 10, 10, 10, 10, *, toUInt128(10), 10, *, 10, NULL IS NULL, 10, 10) ASC NULLS FIRST, x ASC) ORDER BY ALL DESC NULLS FIRST; -- { serverError ILLEGAL_AGGREGATION }

CREATE TABLE t (id Int64, path String) ENGINE = MergeTree ORDER BY path;

SELECT explain FROM (
  SELECT * FROM viewExplain('EXPLAIN', (
    SELECT id FROM t WHERE a
  )))
WHERE equals(id AS a); -- { serverError BAD_ARGUMENTS }
