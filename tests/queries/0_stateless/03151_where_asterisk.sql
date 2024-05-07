SET allow_experimental_analyzer = 1;

SELECT * FROM (SELECT 1) t1 WHERE *; -- { serverError BAD_ARGUMENTS }
