SELECT 1
FORMAT SQLInsert
SETTINGS
    output_format_sql_insert_include_table_schema = 1,
    output_format_sql_insert_table_name = 'a b'; -- { clientError SYNTAX_ERROR }
