SET enable_analyzer = 1;
SELECT 1 FROM (SELECT 1) tx JOIN VALUES ((*)) ty USING (c0); -- { serverError UNKNOWN_IDENTIFIER }
SELECT * FROM numbers(1) AS t1 FULL JOIN numbers(1, 46 AND (1 IS NULL) AND (* AND 3) ) AS t2 USING (number);
