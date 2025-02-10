DROP TABLE IF EXISTS 02834_t;
CREATE TABLE 02834_t (id UInt64, arr Array(UInt64)) ENGINE = MergeTree ORDER BY id;
WITH subquery AS (SELECT []) SELECT t.* FROM 02834_t AS t JOIN subquery ON arrayExists(x -> x = 1, t.arr); -- { serverError INVALID_JOIN_ON_EXPRESSION }
DROP TABLE 02834_t;
