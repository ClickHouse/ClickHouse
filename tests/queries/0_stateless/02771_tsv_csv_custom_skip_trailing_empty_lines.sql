select * from format(TSV, 'x UInt32, y UInt32', '1\t2\n\n') settings input_format_tsv_skip_trailing_empty_lines=0; -- {serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED}
select * from format(TSV, 'x UInt32, y UInt32', '1\t2\n\n') settings input_format_tsv_skip_trailing_empty_lines=1;
select * from format(TSV, 'x UInt32, y UInt32', '1\t2\n\n1\t2\n') settings input_format_tsv_skip_trailing_empty_lines=1; -- {serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED}

select * from format(CSV, 'x UInt32, y UInt32', '1,2\n\n') settings input_format_csv_skip_trailing_empty_lines=0; -- {serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED}
select * from format(CSV, 'x UInt32, y UInt32', '1,2\n\n') settings input_format_csv_skip_trailing_empty_lines=1;
select * from format(CSV, 'x UInt32, y UInt32', '1,2\n\n1,2\n') settings input_format_csv_skip_trailing_empty_lines=1; -- {serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED}

select * from format(CustomSeparated, 'x UInt32, y UInt32', '1\t2\n\n\n') settings input_format_custom_skip_trailing_empty_lines=0; -- {serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED}
select * from format(CustomSeparated, 'x UInt32, y UInt32', '1\t2\n\n\n') settings input_format_custom_skip_trailing_empty_lines=1;
select * from format(CustomSeparated, 'x UInt32, y UInt32', '1\t2\n\n\n1\t2\n\n\n') settings input_format_custom_skip_trailing_empty_lines=1; -- {serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED}

