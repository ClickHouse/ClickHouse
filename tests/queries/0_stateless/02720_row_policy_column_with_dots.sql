CREATE table if not exists table_with_dot_column (date Date, regular_column String, `other_column.2` String) ENGINE = MergeTree() ORDER BY date;
INSERT INTO table_with_dot_column select '2020-01-01', 'Hello', 'World';
INSERT INTO table_with_dot_column select '2024-01-01', 'Hello', 'World';
CREATE ROW POLICY IF NOT EXISTS row_policy ON table_with_dot_column USING toDate(date) >= today() - 30 TO ALL;
SELECT * FROM table_with_dot_column;
DROP TABLE table_with_dot_column;
