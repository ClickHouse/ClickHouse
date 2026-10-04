-- Test short circuit evaluation for OR and AND operators
DROP TABLE IF EXISTS test_03562;
CREATE TABLE test_03562 (id UInt32) ENGINE = Memory;

insert into test_03562 select number from numbers(10000);

set enable_analyzer = 1;
set enable_function_early_short_circuit = 1;
set optimize_trivial_count_query = 0; -- The test asserts on `read_rows` of count subqueries; the trivial count optimization on `Memory` tables would serve them from metadata.

SELECT 'Test OR short circuit with 1 (true) first operand';
SELECT 1 OR (SELECT count(*) FROM test_03562) > 1 AS bool; -- 1

SELECT 'Test AND short circuit with 0 (false) first operand';
SELECT 1 where 0 AND (SELECT count(*) FROM test_03562) > 1 AS bool; -- no result
SELECT 0 AND (SELECT count(*) FROM test_03562) > 1 AS bool; -- 0


SELECT 'Test with string literals';
SELECT 1 where TRUE OR (SELECT count(*) FROM test_03562) > 1 AS bool; -- 1
SELECT true OR (SELECT count(*) FROM test_03562) > 1 AS bool; -- true

SELECT 1 where FALSE AND (SELECT count(*) FROM test_03562) > 1 AS bool; -- no result
SELECT false AND (SELECT count(*) FROM test_03562) > 1 AS bool; -- false

SELECT 'Test with nested OR and AND';
SELECT 1 AND (1 OR (SELECT count(*) FROM test_03562) > 1) AS bool; -- 1
SELECT 0 OR (0 AND (SELECT count(*) FROM test_03562) > 1) AS bool; -- 0
SELECT true AND (true OR (SELECT count(*) FROM test_03562) > 1) AS bool; -- true
SELECT false OR (false AND (SELECT count(*) FROM test_03562) > 1) AS bool; -- false
SELECT 1 OR (0 OR ((SELECT count(*) FROM test_03562) > 0)) AS nested_dead_suffix;

SELECT 'Test type-dependent functions use resolved argument types';
SELECT isNullable(CAST(NULL AS Nullable(UInt8)));
SELECT isNull(CAST(NULL AS Nullable(UInt8)));
SELECT toTypeName(toDateTime64('2020-01-01 00:00:00', 3));
SELECT defaultValueOfArgumentType(CAST(NULL AS Nullable(UInt8))) IS NULL;

SELECT 'Test scoped lambda shadows an optimized builtin function';
WITH ((x, y) -> 42) AS `or` SELECT `or`(1, number) FROM numbers(1);
WITH ((x, y) -> 0) AS `or` SELECT 0 OR `or`(1, 2);
WITH (x -> 'str') AS abs SELECT 1 OR abs(0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'Test folded logical expressions preserve their result types';
SELECT toTypeName(0 AND CAST(NULL AS Nullable(UInt8)));
SELECT toTypeName(1 OR CAST(0 AS Bool));

SELECT 'Test aggregate and arrayJoin branches are not erased';
SELECT 0 AND sum(number) FROM numbers(10);
SELECT 1 OR count() FROM numbers(10);
SELECT 0 AND count() FROM numbers(10);
SELECT 1 OR arrayJoin([1, 2]);

SELECT 'Test JOIN ON scalar subqueries remain planner-safe';
SELECT *
FROM values('l UInt8', 1) AS l
INNER JOIN values('r UInt8', 1) AS r
    ON 1 OR ((SELECT count() FROM system.one) > 0);

SELECT 'Test EXISTS falls back when its runtime value is unknown';
SELECT 0 AND exists(SELECT [1]);
SELECT 1 OR exists(SELECT tuple(1));
SELECT 1 OR tupleElement((10, 20), exists(SELECT * FROM numbers(0))); -- { serverError ARGUMENT_OUT_OF_BOUND, ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'Test scalar cardinality and value-dependent arguments fall back to normal analysis';
SELECT 1 OR ((SELECT number FROM numbers(2)) > 0); -- { serverError INCORRECT_RESULT_OF_SCALAR_SUBQUERY }
SELECT 1 OR ((SELECT count(*) FROM numbers(2) GROUP BY number) > 0); -- { serverError INCORRECT_RESULT_OF_SCALAR_SUBQUERY }
SELECT 1 OR tupleElement((10, 20), assumeNotNull((SELECT 2)));
WITH (SELECT 2) AS idx SELECT 1 OR tupleElement((10, 20), assumeNotNull(idx));
DROP TABLE IF EXISTS test_03562_empty;
CREATE TABLE test_03562_empty (x UInt8) ENGINE = Memory;
SELECT 1 OR intDiv(1, (SELECT count() FROM test_03562_empty)); -- { serverError ILLEGAL_DIVISION }
DROP TABLE test_03562_empty;

SELECT 'Test late table filters in count subqueries fall back to normal analysis';
SELECT 1 OR ((SELECT count() FROM test_03562) > 0)
    SETTINGS additional_table_filters = {'test_03562': 'throwIf(1) = 0'}; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT 1 OR ((SELECT count() FROM test_03562
    SETTINGS additional_table_filters = {'test_03562': 'throwIf(1) = 0'}) > 0); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

SELECT 'Test nested scalars in count subqueries fall back to normal analysis';
SELECT 1 OR ((SELECT count() FROM numbers(assumeNotNull((SELECT 3)))) > 0);
SELECT 1 OR ((SELECT count() FROM numbers(assumeNotNull((SELECT throwIf(1))))) > 0); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
WITH (SELECT 3) AS idx SELECT 1 OR ((SELECT count() FROM numbers(assumeNotNull(idx))) > 0);
WITH (SELECT throwIf(1)) AS idx SELECT 1 OR ((SELECT count() FROM numbers(assumeNotNull(idx))) > 0); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

SELECT 'Test column-dependent count subqueries fall back to normal analysis';
SELECT 1 OR ((SELECT count(*) FROM numbers(1) WHERE number >= 0) > 0);
SELECT 1 OR ((SELECT count(*) FROM numbers(1) WHERE throwIf(number >= 0) = 0) > 0); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

SELECT 'Test CTE table identifiers are not rebound to catalog tables';
DROP TABLE IF EXISTS test_03562_bad;
CREATE TABLE test_03562_bad (x UInt8) ENGINE = Memory;
WITH test_03562_bad AS (SELECT unknown_column FROM system.one)
    SELECT 1 OR ((SELECT count() FROM test_03562_bad) > 0); -- { serverError UNKNOWN_IDENTIFIER }
DROP TABLE test_03562_bad;

SELECT 'Test live prefix arguments are not skipped';
SELECT (SELECT throwIf(1)) OR 1; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT (SELECT throwIf(1)) AND 0; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

SELECT 'Test nondeterministic functions fall back to normal analysis';
SELECT 1 OR ((SELECT count() FROM numbers(1) WHERE throwIf(randConstant() % 1 = 0) = 0) > 0); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

SELECT 'Test comparison non-placeholder expressions stay eager';
SELECT 1 OR ((SELECT count(*) FROM test_03562) > throwIf(1)) AS non_literal_comparison;

SELECT 'Test view-backed count subqueries fall back to normal analysis';
DROP TABLE IF EXISTS test_03562_view_alias;
DROP TABLE IF EXISTS test_03562_view_merge;
DROP VIEW IF EXISTS test_03562_view;
CREATE VIEW test_03562_view AS SELECT number FROM numbers(1) WHERE throwIf(1) = 0;
SELECT 1 OR ((SELECT count() FROM test_03562_view) > 0); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
CREATE TABLE test_03562_view_alias ENGINE = Alias('test_03562_view');
SELECT 1 OR ((SELECT count() FROM test_03562_view_alias) > 0); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
CREATE TABLE test_03562_view_merge ENGINE = Merge(currentDatabase(), '^test_03562_view$');
SELECT 1 OR ((SELECT count() FROM test_03562_view_merge) > 0); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
DROP TABLE test_03562_view_merge;
DROP TABLE test_03562_view_alias;
DROP VIEW test_03562_view;

SELECT 'Test disabled short-circuit evaluation is respected';
SELECT 1 OR (SELECT throwIf(1)) SETTINGS short_circuit_function_evaluation = 'disable'; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

SELECT 'Check the read_rows of the above queries to ensure that the short circuit is working';
SYSTEM FLUSH LOGS query_log;

SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT 1 OR (SELECT count(*) FROM test_03562) > 1 AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT 1 where 0 AND (SELECT count(*) FROM test_03562) > 1 AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT 0 AND (SELECT count(*) FROM test_03562) > 1 AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT 1 where TRUE OR (SELECT count(*) FROM test_03562) > 1 AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT true OR (SELECT count(*) FROM test_03562) > 1 AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT 1 where FALSE AND (SELECT count(*) FROM test_03562) > 1 AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT false AND (SELECT count(*) FROM test_03562) > 1 AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT 1 AND (1 OR (SELECT count(*) FROM test_03562) > 1) AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT 0 OR (0 AND (SELECT count(*) FROM test_03562) > 1) AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT true AND (true OR (SELECT count(*) FROM test_03562) > 1) AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT false OR (false AND (SELECT count(*) FROM test_03562) > 1) AS bool%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT 1 OR (0 OR ((SELECT count(*) FROM test_03562) > 0)) AS nested_dead_suffix%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT 1 OR ((SELECT count(*) FROM test_03562) > throwIf(1)) AS non_literal_comparison%' AND type = 'QueryFinish' AND is_initial_query = 1 ORDER BY event_time DESC LIMIT 1;

SELECT 'Test folded scalar subquery in an aggregate projection';
SELECT DISTINCT (1 OR ((SELECT count(*) FROM test_03562) > 1)), count() IGNORE NULLS AS `count()` FROM test_03562 LIMIT 7;

DROP TABLE IF EXISTS test_03562;
