EXPLAIN SYNTAX INSERT INTO test FROM INFILE 'data.file' SELECT 1; -- { clientError SYNTAX_ERROR }
EXPLAIN SYNTAX INSERT INTO test FROM INFILE 'data.file' WATCH view; -- { clientError SYNTAX_ERROR }
EXPLAIN SYNTAX INSERT INTO test FROM INFILE 'data.file' VALUES (1) -- { clientError SYNTAX_ERROR }
EXPLAIN SYNTAX INSERT INTO test FROM INFILE 'data.file' WITH number AS x SELECT number FROM numbers(10); -- { clientError SYNTAX_ERROR }
