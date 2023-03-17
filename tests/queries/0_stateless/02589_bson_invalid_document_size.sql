set input_format_parallel_parsing=1;
select * from format(BSONEachRow, 'x UInt32', x'00000000'); -- {serverError INCORRECT_DATA}
