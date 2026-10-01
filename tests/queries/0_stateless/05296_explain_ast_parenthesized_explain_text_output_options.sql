-- Output options after a parenthesized `EXPLAIN TEXT` belong to the enclosing `EXPLAIN`, so the
-- parentheses are kept when the query is formatted, and it parses back the same.
SELECT formatQuerySingleLine('EXPLAIN AST (EXPLAIN TEXT SELECT 1) FORMAT TSV');
SELECT formatQuerySingleLine('EXPLAIN AST (EXPLAIN TEXT SELECT 1 ONELINE) SETTINGS max_threads = 1');
SELECT formatQuerySingleLine('EXPLAIN AST (EXPLAIN TEXT SELECT 1) INTO OUTFILE ''f''');
SELECT formatQuerySingleLine('EXPLAIN AST EXPLAIN AST (EXPLAIN TEXT SELECT 1) FORMAT TSV');
SELECT formatQuerySingleLine('EXPLAIN TEXT (EXPLAIN AST (EXPLAIN TEXT SELECT 1) FORMAT TSV) ONELINE');

-- Without parentheses the nested `EXPLAIN TEXT` takes the options, and none are added.
SELECT formatQuerySingleLine('EXPLAIN AST EXPLAIN TEXT SELECT 1 FORMAT TSV');

SELECT q, parseQueryToJSON(formatQuerySingleLine(q)) = parseQueryToJSON(q)
FROM
(
    SELECT arrayJoin([
        'EXPLAIN AST (EXPLAIN TEXT SELECT 1) FORMAT TSV',
        'EXPLAIN AST (EXPLAIN TEXT SELECT 1 ONELINE) SETTINGS max_threads = 1',
        'EXPLAIN AST (EXPLAIN TEXT SELECT 1) INTO OUTFILE ''f''',
        'EXPLAIN AST graph = 0 (EXPLAIN TEXT SELECT 1) FORMAT TSV',
        'EXPLAIN AST EXPLAIN AST (EXPLAIN TEXT SELECT 1) FORMAT TSV',
        'EXPLAIN TEXT (EXPLAIN AST (EXPLAIN TEXT SELECT 1) FORMAT TSV) ONELINE',
        'EXPLAIN AST EXPLAIN TEXT SELECT 1 FORMAT TSV']) AS q
);
