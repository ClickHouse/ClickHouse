SET allow_experimental_analyzer = 1;

SELECT NULL GROUP BY tuple('0.0000000007'), count(NULL) OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) -- { serverError 184 };
