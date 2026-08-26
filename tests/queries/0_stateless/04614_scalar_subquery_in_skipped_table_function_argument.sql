-- Scalar subqueries inside a table function argument that fails to resolve on the
-- initiator (e.g. a sharding key referencing a remote-side column) and is kept
-- unresolved by the `UNKNOWN_IDENTIFIER` fallback in `resolveTableFunction`.
-- Scalars in table function arguments are executed for real even in only-analyze
-- mode (see 04516_scalar_subquery_as_table_function_argument), so a scalar that is
-- not a valid scalar fails during CREATE TABLE ... AS SELECT header inference with
-- the same error as in a plain SELECT.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS dst_single;

SELECT '-- single-row scalar in a skipped sharding-key argument';
SELECT * FROM remote('localhost', view(SELECT 2 AS x), cityHash64(assumeNotNull(concat(x, (SELECT '1'))))) ORDER BY x;

SELECT '-- the same in CREATE TABLE ... AS SELECT';
CREATE TABLE dst_single ENGINE = Memory AS
SELECT * FROM remote('localhost', view(SELECT 2 AS x), cityHash64(assumeNotNull(concat(x, (SELECT '1')))));
SELECT * FROM dst_single ORDER BY x;

SELECT '-- multi-row scalar in a skipped sharding-key argument is an error';
SELECT * FROM remote('localhost', view(SELECT 2 AS x), cityHash64(assumeNotNull((SELECT number FROM numbers(2)) + x))); -- { serverError INCORRECT_RESULT_OF_SCALAR_SUBQUERY }
CREATE TABLE dst_multi ENGINE = Memory AS
SELECT * FROM remote('localhost', view(SELECT 2 AS x), cityHash64(assumeNotNull((SELECT number FROM numbers(2)) + x))); -- { serverError INCORRECT_RESULT_OF_SCALAR_SUBQUERY }

DROP TABLE dst_single;
DROP TABLE IF EXISTS dst_multi;
