select * from numbers(100) settings max_result_rows = 1; -- { serverError 396 }
select * from numbers(100) FORMAT JSON settings max_result_rows = 1; -- { serverError 396 }

SET max_result_rows = 1;
select * from numbers(10); -- { serverError 396 }
select * from numbers(10) SETTINGS result_overflow_mode = 'break', max_block_size = 1 FORMAT PrettySpaceNoEscapes;
select * from numbers(10) settings max_result_rows = 10;
select * from numbers(10) FORMAT JSONCompact settings max_result_rows = 10, output_format_write_statistics = 0;
