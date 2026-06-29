SET enable_analyzer = 1;

-- { echo }

SELECT CASE WHEN number = 0 THEN (SELECT 0) WHEN number = 1 THEN (SELECT 1) END as col, toTypeName(col) FROM numbers(2);

SELECT CASE WHEN number = 0 THEN (SELECT 1, 2) WHEN number = 1 THEN (SELECT 3, 4) END as col, toTypeName(col) FROM numbers(2);

SELECT CASE WHEN number = 0 THEN (SELECT 1, 2) WHEN number = 1 THEN (SELECT NULL) END as col, toTypeName(col) FROM numbers(2);

SELECT CASE WHEN number = 0 THEN (SELECT NULL) WHEN number = 1 THEN (SELECT 1, 2) END as col, toTypeName(col) FROM numbers(2);

SELECT CASE WHEN number = 0 THEN (SELECT NULL) WHEN number = 1 THEN (SELECT NULL) END as col, toTypeName(col) FROM numbers(2);
