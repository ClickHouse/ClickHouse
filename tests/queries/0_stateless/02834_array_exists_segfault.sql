DROP TABLE IF EXISTS 02834_t;
CREATE TABLE 02834_t (id UInt64, arr Array(UInt64)) ENGINE = MergeTree ORDER BY id;
SET enable_analyzer = 0;
WITH subquery AS (SELECT []) SELECT t.* FROM 02834_t AS t JOIN subquery ON arrayExists(x -> x = 1, t.arr); -- { serverError INVALID_JOIN_ON_EXPRESSION }
SET enable_analyzer = 1;
WITH subquery AS (SELECT []) SELECT t.* FROM 02834_t AS t JOIN subquery ON arrayExists(x -> x = 1, t.arr);
INSERT INTO 02834_t VALUES (1, [1]), (2, [2]), (3, [1, 3]);
WITH subquery AS (SELECT []) SELECT t.* FROM 02834_t AS t JOIN subquery ON arrayExists(x -> x = 1, t.arr) ORDER BY t.id;
DROP TABLE 02834_t;
