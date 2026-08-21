SELECT (1, (1 AS c0, 1 AS c0) IS NULL AS c0), (1, (1, 1 AS c0) IS NULL AS c0) IS NULL AS c0; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT (1,1) c0, (1,(1)) c0; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT (-5 AS b) IN (-5 AS b); -- should return 1
