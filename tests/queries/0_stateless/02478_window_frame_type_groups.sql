-- GROUPS frame is supported. With ORDER BY a constant, all rows form a single
-- peer group, so dense_rank() is 1 for every row.

SET enable_analyzer = 0;

SELECT toUInt64(dense_rank() OVER (ORDER BY 100 ASC GROUPS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) FROM numbers(10);

SET enable_analyzer = 1;

SELECT toUInt64(dense_rank() OVER (ORDER BY 100 ASC GROUPS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) FROM numbers(10);
