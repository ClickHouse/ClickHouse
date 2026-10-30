-- https://antonz.org/sql-fetch/

CREATE TEMPORARY TABLE employees (id UInt64, name String, department String, salary UInt64);
INSERT INTO employees VALUES (23, 'Henry', 'it', 104), (24, 'Irene', 'it', 104), (25, 'Frank', 'it', 120), (31, 'Cindy', 'sales', 96), (33, 'Alice', 'sales', 100), (32, 'Dave', 'sales', 96), (22, 'Grace', 'it', 90), (21, 'Emma', 'it', 84);

-- Determinism
SET max_threads = 1, parallelize_output_from_storages = 0;

select transform(name, ['Henry', 'Irene', 'Dave', 'Cindy'], ['Henry or Irene', 'Henry or Irene', 'Dave or Cindy', 'Dave or Cindy']) AS name, department, salary from (SELECT * FROM employees ORDER BY id, name, department, salary)
order by salary desc
limit 5
format PrettyCompactNoEscapes;

select transform(name, ['Henry', 'Irene', 'Dave', 'Cindy'], ['Henry or Irene', 'Henry or Irene', 'Dave or Cindy', 'Dave or Cindy']) AS name, department, salary from (SELECT * FROM employees ORDER BY id, name, department, salary)
order by salary desc
fetch first 5 rows only
format PrettyCompactNoEscapes;

select transform(name, ['Henry', 'Irene', 'Dave', 'Cindy'], ['Henry or Irene', 'Henry or Irene', 'Dave or Cindy', 'Dave or Cindy']) AS name, department, salary from (SELECT * FROM employees ORDER BY id, name, department, salary)
order by salary desc
fetch first 5 rows with ties
format PrettyCompactNoEscapes;

select transform(name, ['Henry', 'Irene', 'Dave', 'Cindy'], ['Henry or Irene', 'Henry or Irene', 'Dave or Cindy', 'Dave or Cindy']) AS name, department, salary from (SELECT * FROM employees ORDER BY id, name, department, salary)
order by salary desc
offset 3 rows
fetch next 5 rows only
format PrettyCompactNoEscapes;

select transform(name, ['Henry', 'Irene', 'Dave', 'Cindy'], ['Henry or Irene', 'Henry or Irene', 'Dave or Cindy', 'Dave or Cindy']) AS name, department, salary from (SELECT * FROM employees ORDER BY id, name, department, salary)
order by salary desc
offset 3 rows
fetch first 5 rows only
format PrettyCompactNoEscapes;

-- The SQL standard makes fetch_row_count optional and defaults it to 1.
SELECT number FROM numbers(3) ORDER BY number FETCH FIRST ROW ONLY;
SELECT number FROM numbers(3) ORDER BY number FETCH FIRST ROWS ONLY;
SELECT number FROM numbers(3) ORDER BY number OFFSET 1 ROWS FETCH NEXT ROW ONLY;
SELECT number % 2 FROM numbers(4) ORDER BY number % 2 FETCH FIRST ROW WITH TIES;

-- ROW and ROWS are contextual keywords, so they can still be explicit count expressions.
WITH 2 AS row SELECT number FROM numbers(3) ORDER BY number FETCH FIRST row ROWS ONLY;
WITH 2 AS rows SELECT number FROM numbers(3) ORDER BY number FETCH FIRST rows ROWS ONLY;
