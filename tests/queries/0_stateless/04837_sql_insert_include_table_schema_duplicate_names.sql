SELECT 1 AS x, 1 AS x
FORMAT SQLInsert
SETTINGS output_format_sql_insert_include_table_schema = 1; -- { clientError DUPLICATE_COLUMN }
