SET allow_experimental_analyzer = 0;

SELECT toUInt64(dense_rank(1) OVER (ORDER BY 100 ASC GROUPS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) FROM numbers(10); -- { serverError 48 }

SET allow_experimental_analyzer = 1;

SELECT toUInt64(dense_rank(1) OVER (ORDER BY 100 ASC GROUPS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) FROM numbers(10); -- { serverError 48 }
