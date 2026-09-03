-- Random settings limits: min_chunk_bytes_for_parallel_parsing=(10485760, None)
drop table if exists tsv;
set output_format_parallel_formatting=1;
set max_read_buffer_size=1048576;
-- Keep max_block_size far below rows/(max_threads+2) so the formatter's ring of
-- processing units wraps and unit reuse is exercised.
set max_block_size=1000;

create table tsv(a int, b int default 7) engine File(TSV);

insert into tsv(a) select number from numbers(1000000);
select '1000000';
select count() from tsv;


insert into tsv(a) select number from numbers(1000000);
select '2000000';
select count() from tsv;


insert into tsv(a) select number from numbers(1000000);
select '3000000';
select count() from tsv;


drop table tsv;
