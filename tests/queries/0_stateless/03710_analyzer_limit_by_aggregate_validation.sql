SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS test_limit_by_validation;
CREATE TABLE test_limit_by_validation (c0 Int32, c1 Int32, c2 Int32) ENGINE = Memory;
INSERT INTO test_limit_by_validation VALUES (1, 10, 100), (1, 20, 200), (2, 30, 300);

SELECT c0 FROM test_limit_by_validation GROUP BY c0 LIMIT 1 BY c1; -- { serverError NOT_AN_AGGREGATE }

SELECT c0, sum(c2) as s FROM test_limit_by_validation GROUP BY c0 LIMIT 1 BY c1; -- { serverError NOT_AN_AGGREGATE }

SELECT c0, c1 FROM test_limit_by_validation GROUP BY c0, c1 ORDER BY c0, c1 LIMIT 1 BY c1;

SELECT c0, sum(c1) as s FROM test_limit_by_validation GROUP BY c0 ORDER BY c0 LIMIT 1 BY c0;

SELECT c0 + 1 as expr FROM test_limit_by_validation GROUP BY c0 + 1 ORDER BY expr LIMIT 1 BY expr;

DROP TABLE test_limit_by_validation;
