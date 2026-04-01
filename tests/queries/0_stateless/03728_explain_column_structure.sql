SET query_plan_merge_expressions = 1;

SELECT * FROM (EXPLAIN PLAN header = 1, input_headers = 1 SELECT 1) WHERE explain NOT LIKE 'Expression%';
SELECT * FROM (EXPLAIN PLAN header = 1, input_headers = 1, column_structure = 1 SELECT 1) WHERE explain NOT LIKE 'Expression%';