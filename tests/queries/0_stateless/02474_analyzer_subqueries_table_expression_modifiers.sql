SET allow_experimental_analyzer = 1;

SELECT * FROM (SELECT 1) FINAL; -- { serverError 1 }
SELECT * FROM (SELECT 1) SAMPLE 1/2; -- { serverError 1 }
SELECT * FROM (SELECT 1) FINAL SAMPLE 1/2; -- { serverError 1 }

WITH cte_subquery AS (SELECT 1) SELECT * FROM cte_subquery FINAL; -- { serverError 1 }
WITH cte_subquery AS (SELECT 1) SELECT * FROM cte_subquery SAMPLE 1/2; -- { serverError 1 }
WITH cte_subquery AS (SELECT 1) SELECT * FROM cte_subquery FINAL SAMPLE 1/2; -- { serverError 1 }

SELECT * FROM (SELECT 1 UNION ALL SELECT 1) FINAL; -- { serverError 1 }
SELECT * FROM (SELECT 1 UNION ALL SELECT 1) SAMPLE 1/2; -- { serverError 1 }
SELECT * FROM (SELECT 1 UNION ALL SELECT 1) FINAL SAMPLE 1/2; -- { serverError 1 }

WITH cte_subquery AS (SELECT 1 UNION ALL SELECT 1) SELECT * FROM cte_subquery FINAL; -- { serverError 1 }
WITH cte_subquery AS (SELECT 1 UNION ALL SELECT 1) SELECT * FROM cte_subquery SAMPLE 1/2; -- { serverError 1 }
WITH cte_subquery AS (SELECT 1 UNION ALL SELECT 1) SELECT * FROM cte_subquery FINAL SAMPLE 1/2; -- { serverError 1 }
