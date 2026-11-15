select * from format(CSV, 'd DateTime64(3)', '1744042005 797') settings date_time_input_format='best_effort'; -- {serverError UNEXPECTED_DATA_AFTER_PARSED_VALUE}
select * from format(CSV, 'd DateTime', '1744042005 797') settings date_time_input_format='best_effort'; -- {serverError UNEXPECTED_DATA_AFTER_PARSED_VALUE}

