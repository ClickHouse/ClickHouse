-- A bare trailing `FORMAT` belongs to `EXPLAIN TEXT`.
EXPLAIN TEXT INSERT INTO t SELECT 1 FORMAT JSONEachRow;

-- Parentheses preserve a source-owned `FORMAT`.
EXPLAIN TEXT (INSERT INTO t SELECT 1 FORMAT CSV) FORMAT JSONEachRow;

-- Actions delimit the source and its output options.
EXPLAIN TEXT INSERT INTO t SELECT 1 FORMAT CSV ONELINE FORMAT JSONEachRow;

-- Two consecutive `FORMAT` clauses preserve both owners.
EXPLAIN TEXT INSERT INTO t SELECT 1 FORMAT CSV FORMAT JSONEachRow;

-- Bare and explicitly delimited outer formats produce the same AST.
SELECT
    parseQueryToJSON('EXPLAIN TEXT INSERT INTO t SELECT 1 FORMAT JSONEachRow')
    = parseQueryToJSON('EXPLAIN TEXT (INSERT INTO t SELECT 1) FORMAT JSONEachRow');

-- An input format remains on the source.
SELECT
    parseQueryToJSON(
        'EXPLAIN TEXT INSERT INTO t FROM INFILE ''unused.csv'' SELECT 1 FORMAT CSV')
    = parseQueryToJSON(
        'EXPLAIN TEXT (INSERT INTO t FROM INFILE ''unused.csv'' SELECT 1 FORMAT CSV)');

-- Relocation must not rewind past settings already consumed by the insert parser.
SELECT
    parseQueryToJSON(
        'EXPLAIN TEXT INSERT INTO t SELECT 1 FORMAT JSONEachRow SETTINGS max_threads = 2')
    = parseQueryToJSON(
        'EXPLAIN TEXT (INSERT INTO t SETTINGS max_threads = 2 SELECT 1) FORMAT JSONEachRow')
SETTINGS allow_settings_after_format_in_insert = 1;

-- SQL and JSON formatting must preserve the original AST.
-- `INTO OUTFILE` occurs only inside a string passed to the parser.
SELECT
    count(),
    min(parseQueryToJSON(formatQuerySingleLine(q)) = parseQueryToJSON(q)),
    min(parseQueryToJSON(formatQuery(q)) = parseQueryToJSON(q)),
    min(parseQueryToJSON(formatQueryFromJSON(parseQueryToJSON(q))) = parseQueryToJSON(q))
FROM
(
    SELECT arrayJoin([
        'EXPLAIN TEXT INSERT INTO t SELECT 1 FORMAT JSONEachRow',
        'EXPLAIN TEXT (INSERT INTO t SELECT 1) FORMAT JSONEachRow',
        'EXPLAIN TEXT (INSERT INTO t SELECT 1 FORMAT CSV)',
        'EXPLAIN TEXT INSERT INTO t SELECT 1 FORMAT CSV FORMAT JSONEachRow',
        'EXPLAIN TEXT INSERT INTO t SELECT 1 FORMAT CSV ONELINE FORMAT JSONEachRow',
        'EXPLAIN TEXT (INSERT INTO t SELECT 1 FORMAT CSV) FORMAT JSONEachRow',
        'EXPLAIN TEXT (INSERT INTO t SELECT 1) SETTINGS max_threads = 2',
        'EXPLAIN TEXT (INSERT INTO t SELECT 1) INTO OUTFILE ''unused.out'' FORMAT JSONEachRow',
        'EXPLAIN TEXT (EXPLAIN TEXT (SELECT 1) ONELINE) MULTILINE',
        'EXPLAIN TEXT SELECT 1 EXCEPT ALL SELECT 2 ONELINE',
        'EXPLAIN TEXT SELECT 1 EXCEPT DISTINCT SELECT 2',
        'EXPLAIN TEXT ((SELECT 1 INTERSECT ALL SELECT 2) INTERSECT ALL SELECT 3 UNION ALL SELECT 4) ONELINE FORMAT JSONEachRow'
    ]) AS q
);