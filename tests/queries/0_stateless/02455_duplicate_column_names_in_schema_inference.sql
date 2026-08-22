-- Tags: no-fasttest

desc format(JSONEachRow, '{"x" : 1, "x" : 2}'); -- {serverError CANNOT_EXTRACT_TABLE_STRUCTURE}
desc format(JSONEachRow, '{"x" : 1, "y" : 2}\n{"x" : 2, "x" : 3}'); -- {serverError CANNOT_EXTRACT_TABLE_STRUCTURE}
desc format(CSVWithNames, 'a,b,a\n1,2,3'); -- {serverError CANNOT_EXTRACT_TABLE_STRUCTURE}
desc format(CSV, '1,2,3') settings column_names_for_schema_inference='a, b, a'; -- {serverError CANNOT_EXTRACT_TABLE_STRUCTURE}

