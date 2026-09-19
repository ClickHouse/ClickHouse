EXPLAIN TEXT (INSERT INTO t SELECT * FROM input('x UInt8') FORMAT CSV) ONELINE;

WITH 'EXPLAIN TEXT INSERT INTO t SELECT * FROM input(''x UInt8'') FORMAT CSV ONELINE' AS q
SELECT
    formatQuery(formatQuery(q)) = formatQuery(q),
    parseQueryToJSON(formatQuery(q)) = parseQueryToJSON(q);

SELECT formatQuery('EXPLAIN TEXT (INSERT INTO t SELECT * FROM input(''x UInt8'') FORMAT CSV\n1\n) ONELINE'); -- { serverError BAD_ARGUMENTS }
