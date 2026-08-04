-- This test reads back 593 MB through the TSV parser, so a min_chunk_bytes_for_parallel_parsing
-- below the default multiplies the parse segments and the test reaches the 600 s timeout.
-- Random settings limits: min_chunk_bytes_for_parallel_parsing=(10485760, None)
drop table if exists tsv;
set output_format_parallel_formatting=1;
set max_read_buffer_size=1048576;
set max_block_size=65505;

create table tsv(a int, b int default 7) engine File(TSV);

insert into tsv(a) select number from numbers(10000000);
select '10000000';
select count() from tsv;


insert into tsv(a) select number from numbers(10000000);
select '20000000';
select count() from tsv;


insert into tsv(a) select number from numbers(10000000);
select '30000000';
select count() from tsv;


drop table tsv;
