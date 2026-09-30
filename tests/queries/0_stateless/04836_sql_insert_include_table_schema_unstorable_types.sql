SELECT NULL AS x
FORMAT SQLInsert
SETTINGS output_format_sql_insert_include_table_schema = 1; -- { clientError DATA_TYPE_CANNOT_BE_USED_IN_TABLES }

SELECT [] AS x
FORMAT SQLInsert
SETTINGS output_format_sql_insert_include_table_schema = 1; -- { clientError DATA_TYPE_CANNOT_BE_USED_IN_TABLES }
