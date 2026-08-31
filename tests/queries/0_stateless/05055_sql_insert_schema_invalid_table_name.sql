SELECT 1
FORMAT SQLInsert
SETTINGS
    output_format_sql_insert_include_table_schema = 1,
    output_format_sql_insert_table_name = 'a b'; -- { clientError SYNTAX_ERROR }

SELECT toUInt8(1) AS x
FORMAT SQLInsert
SETTINGS
    output_format_sql_insert_include_table_schema = 1,
    output_format_sql_insert_table_name = 'sql_insert_schema_comment_05055 -- ignored';
