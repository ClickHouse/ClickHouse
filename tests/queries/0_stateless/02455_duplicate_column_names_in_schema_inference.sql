-- Tags: no-fasttest

desc format(JSONEachRow, '{"x" : 1, "x" : 2}'); -- {serverError INCORRECT_DATA}
desc format(JSONEachRow, '{"x" : 1, "y" : 2}\n{"x" : 2, "x" : 3}'); -- {serverError INCORRECT_DATA}
desc format(CSVWithNames, 'a,b,a\n1,2,3'); -- {serverError INCORRECT_DATA}
desc format(CSV, '1,2,3') settings column_names_for_schema_inference='a, b, a'; -- {serverError INCORRECT_DATA}

