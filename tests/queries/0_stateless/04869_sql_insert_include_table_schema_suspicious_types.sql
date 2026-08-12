SELECT toLowCardinality(toUInt8(1)) AS x
FORMAT SQLInsert
SETTINGS output_format_sql_insert_include_table_schema = 1; -- { clientError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY }
