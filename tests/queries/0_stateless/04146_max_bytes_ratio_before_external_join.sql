SELECT * FROM numbers(10) AS t1 INNER JOIN numbers(10) AS t2 ON t1.number = t2.number FORMAT Null SETTINGS max_bytes_ratio_before_external_join = -0.1; -- { serverError BAD_ARGUMENTS }
SELECT * FROM numbers(10) AS t1 INNER JOIN numbers(10) AS t2 ON t1.number = t2.number FORMAT Null SETTINGS max_bytes_ratio_before_external_join = 1; -- { serverError BAD_ARGUMENTS }

-- Valid ratio: small in-memory join still produces the expected count.
SELECT count() FROM numbers(100) AS t1 INNER JOIN numbers(100) AS t2 ON t1.number = t2.number SETTINGS max_bytes_ratio_before_external_join = 0.5;
