set output_format_pretty_display_footer_column_names=0;
set output_format_write_statistics=0;

select * from numbers(100) settings max_result_rows = 1; -- { serverError TOO_MANY_ROWS_OR_BYTES }
select * from numbers(100) FORMAT JSON settings max_result_rows = 1; -- { serverError TOO_MANY_ROWS_OR_BYTES }
select * from numbers(100) FORMAT TSVWithNamesAndTypes settings max_result_rows = 1; -- { serverError TOO_MANY_ROWS_OR_BYTES }
select * from numbers(100) FORMAT CSVWithNamesAndTypes settings max_result_rows = 1; -- { serverError TOO_MANY_ROWS_OR_BYTES }
select * from numbers(100) FORMAT JSONCompactEachRowWithNamesAndTypes settings max_result_rows = 1; -- { serverError TOO_MANY_ROWS_OR_BYTES }
select * from numbers(100) FORMAT XML settings max_result_rows = 1; -- { serverError TOO_MANY_ROWS_OR_BYTES }

SET max_result_rows = 1;
select * from numbers(10); -- { serverError TOO_MANY_ROWS_OR_BYTES }
select * from numbers(10) SETTINGS result_overflow_mode = 'break', max_block_size = 1 FORMAT PrettySpaceNoEscapes;
select * from numbers(10) settings max_result_rows = 10;
select * from numbers(10) FORMAT JSONCompact settings max_result_rows = 10, output_format_write_statistics = 0;
