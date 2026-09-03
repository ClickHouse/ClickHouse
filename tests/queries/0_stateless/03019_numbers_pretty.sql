SET output_format_pretty_row_numbers = 0;
SELECT 1.23e9 FORMAT Pretty;
SELECT -1.23e9 FORMAT Pretty;
SELECT inf FORMAT Pretty;
SELECT -inf FORMAT Pretty;
SELECT nan FORMAT Pretty;
SELECT 1e111 FORMAT Pretty;
