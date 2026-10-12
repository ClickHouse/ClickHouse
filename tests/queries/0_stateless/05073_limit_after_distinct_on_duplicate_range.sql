-- `DISTINCT ON` is lowered to a hidden `LIMIT BY`, which allows a second `LIMIT` clause. A range in the
-- first `LIMIT` is a complete limit already, so a second `LIMIT` is a syntax error rather than an
-- overwrite of the first clause.
SELECT DISTINCT ON (number % 2) number AS x FROM numbers(6) ORDER BY x LIMIT AFTER x >= 1 LIMIT AFTER x >= 0; -- { clientError SYNTAX_ERROR }
SELECT DISTINCT ON (number % 2) number AS x FROM numbers(6) ORDER BY x LIMIT 1 UNTIL x >= 1 LIMIT 2; -- { clientError SYNTAX_ERROR }
SELECT DISTINCT ON (number % 2) number AS x FROM numbers(6) ORDER BY x LIMIT AFTER x >= 1;
