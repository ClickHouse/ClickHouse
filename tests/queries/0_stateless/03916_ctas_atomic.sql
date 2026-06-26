DROP TABLE IF EXISTS test_ctas_failed;
CREATE TABLE test_ctas_failed ENGINE = MergeTree() ORDER BY n AS
SELECT number as n FROM numbers(10) WHERE throwIf(n = 4); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT name FROM system.tables WHERE name = 'test_ctas_failed' AND database = currentDatabase();

DROP TABLE IF EXISTS test_ctas_ifne;
CREATE TABLE test_ctas_ifne ENGINE = MergeTree() ORDER BY n AS SELECT number as n FROM numbers(5);
CREATE TABLE IF NOT EXISTS test_ctas_ifne ENGINE = MergeTree() ORDER BY n AS SELECT number as n FROM numbers(10);
SELECT count() FROM test_ctas_ifne; 
