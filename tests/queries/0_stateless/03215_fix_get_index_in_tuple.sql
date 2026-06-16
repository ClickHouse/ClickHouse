CREATE TABLE IF NOT EXISTS dummy_table_03215
(
    id_col Nullable(String),
    date_col Date
)
    ENGINE = MergeTree()
    ORDER BY date_col;

INSERT INTO dummy_table_03215 (id_col, date_col)
    VALUES ('some_string', '2024-05-21');

SELECT 0 as _row_exists
FROM dummy_table_03215
WHERE (date_col, id_col) IN (SELECT (date_col, id_col) FROM dummy_table_03215);

DROP TABLE IF EXISTS dummy_table_03215;
