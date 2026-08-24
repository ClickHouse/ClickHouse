set input_format_parallel_parsing=1;
set max_threads=0;
select * from format(BSONEachRow, 'x UInt32', x'00000000'); -- {serverError INCORRECT_DATA}

