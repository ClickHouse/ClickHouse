DESC format(CSV, '1E20\n1.1E20') settings input_format_try_infer_exponent_floats = 0;
DESC format(CSV, '1E20\n1.1E20') settings input_format_try_infer_exponent_floats = 1;
-- This setting should not take affect on JSON formats
DESC format(JSONEachRow, '{"x" : 1.1e20}') settings input_format_try_infer_exponent_floats = 0;

