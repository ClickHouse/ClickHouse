CREATE TEMPORARY TABLE my_table (col_date Date, col_date32 Date32, col_datetime DateTime, col_datetime32 DateTime32, col_datetime64 DateTime64);
insert into `my_table` (`col_date`, `col_date32`, `col_datetime`, `col_datetime32`, `col_datetime64`) values (parseDateTime64BestEffort('1969-01-01'), '1969-01-01', parseDateTime64BestEffort('1969-01-01 10:42:00'), parseDateTime64BestEffort('1969-01-01 10:42:00'), parseDateTime64BestEffort('1969-01-01 10:42:00'));

-- The values for Date32 and DateTime64 will be year 1969, while the values of Date, DateTime will contain a value affected by implementation-defined overflow and can be arbitrary.
SELECT * APPLY(x -> (toTypeName(x), x)) FROM my_table;
