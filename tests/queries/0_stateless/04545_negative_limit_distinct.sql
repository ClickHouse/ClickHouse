-- Negative LIMIT combined with DISTINCT must return the tail of the ordered result,
-- not the head. A multi-part MergeTree enables the in-order DISTINCT whose limit hint
-- previously ignored the negative sign and truncated to the first |LIMIT| distinct rows.
-- https://github.com/ClickHouse/ClickHouse/issues/111254

DROP TABLE IF EXISTS nl;
CREATE TABLE nl (x Int64) ENGINE = MergeTree ORDER BY x;
-- Four separate parts are required: with a single part the negative LIMIT is correct
-- even on the buggy build, so stop merges to keep the parts from collapsing.
SYSTEM STOP MERGES nl;
INSERT INTO nl SELECT number       FROM numbers(100);
INSERT INTO nl SELECT number + 100 FROM numbers(100);
INSERT INTO nl SELECT number + 200 FROM numbers(100);
INSERT INTO nl SELECT number + 300 FROM numbers(100);

SET enable_analyzer = 1;
SELECT 'analyzer';
SELECT DISTINCT intDiv(x, 100) AS d FROM nl ORDER BY d LIMIT -1;          -- 3
SELECT DISTINCT intDiv(x, 100) AS d FROM nl ORDER BY d LIMIT -2;          -- 2 3
SELECT DISTINCT intDiv(x, 100) AS d FROM nl ORDER BY d LIMIT -3;          -- 1 2 3
SELECT DISTINCT intDiv(x, 100) AS d FROM nl ORDER BY d LIMIT -1 OFFSET 1; -- 3
SELECT DISTINCT intDiv(x, 100) AS d FROM nl ORDER BY d LIMIT 1;           -- 0 (positive unchanged)
SELECT DISTINCT intDiv(x, 100) AS d FROM nl ORDER BY d LIMIT 2;           -- 0 1 (positive unchanged)

SET enable_analyzer = 0;
SELECT 'old analyzer';
SELECT DISTINCT intDiv(x, 100) AS d FROM nl ORDER BY d LIMIT -1;          -- 3
SELECT DISTINCT intDiv(x, 100) AS d FROM nl ORDER BY d LIMIT -2;          -- 2 3
SELECT DISTINCT intDiv(x, 100) AS d FROM nl ORDER BY d LIMIT 1;           -- 0 (positive unchanged)

DROP TABLE nl;
