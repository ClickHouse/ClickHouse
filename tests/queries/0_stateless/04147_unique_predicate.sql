SET enable_analyzer = 1;

SELECT 'All distinct rows';
SELECT UNIQUE(SELECT number FROM numbers(5));

SELECT 'Duplicate rows';
SELECT UNIQUE(SELECT number % 3 FROM numbers(6));

SELECT 'Empty subquery (vacuously unique)';
SELECT UNIQUE(SELECT number FROM numbers(0));

SELECT 'Single row';
SELECT UNIQUE(SELECT 1);

SELECT 'All same values';
SELECT UNIQUE(SELECT 1 FROM numbers(3));

SELECT 'Multiple columns, all distinct';
SELECT UNIQUE(SELECT number, number * 10 FROM numbers(5));

SELECT 'Multiple columns with duplicates';
SELECT UNIQUE(SELECT number % 2, number % 3 FROM numbers(12));

SELECT 'UNIQUE in WHERE clause';
SELECT 'in_where_true' WHERE UNIQUE(SELECT number FROM numbers(3));
SELECT 'in_where_false' WHERE UNIQUE(SELECT 1 FROM numbers(3));

SELECT 'NULL rows are never duplicates (SQL standard)';
SELECT UNIQUE(SELECT NULL FROM numbers(3));
SELECT UNIQUE(SELECT if(number % 2 = 0, NULL, number) FROM numbers(5));

SELECT 'Multiple columns with NULLs: rows with any NULL column are skipped';
SELECT UNIQUE(SELECT number, if(number = 2, NULL, number) FROM numbers(5));

SELECT 'Nullable column with actual duplicates among non-NULL rows';
SELECT UNIQUE(SELECT if(number < 3, number % 2, NULL) FROM numbers(5));

SELECT 'LowCardinality(Nullable(...)) columns';
DROP TABLE IF EXISTS t_lc_nullable;
CREATE TABLE t_lc_nullable (x LowCardinality(Nullable(String))) ENGINE = Memory;
INSERT INTO t_lc_nullable VALUES ('a'), (NULL), ('b'), (NULL);
SELECT UNIQUE(SELECT x FROM t_lc_nullable);
INSERT INTO t_lc_nullable VALUES ('a');
SELECT UNIQUE(SELECT x FROM t_lc_nullable);
DROP TABLE t_lc_nullable;

SELECT 'Correlated subquery: not supported';
SELECT UNIQUE(SELECT number FROM numbers(5) WHERE number = n) FROM (SELECT 1 AS n); -- { serverError NOT_IMPLEMENTED }
