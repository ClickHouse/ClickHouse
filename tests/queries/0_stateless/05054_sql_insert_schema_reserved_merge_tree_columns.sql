SELECT 1 AS _row_exists
FORMAT SQLInsert
SETTINGS output_format_sql_insert_include_table_schema = 1; -- { clientError ILLEGAL_COLUMN }

SELECT 1 AS _block_number
FORMAT SQLInsert
SETTINGS output_format_sql_insert_include_table_schema = 1; -- { clientError ILLEGAL_COLUMN }

SELECT 1 AS _block_offset
FORMAT SQLInsert
SETTINGS output_format_sql_insert_include_table_schema = 1; -- { clientError ILLEGAL_COLUMN }
