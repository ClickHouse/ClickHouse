-- Tags: no-fasttest

insert into function file(currentDatabase() || '_03762_tuple.parquet') select tuple(42, [tuple(44, 'hello')])::Tuple(a UInt32, b Array(Tuple(c UInt32, d String))) as data settings engine_file_truncate_on_insert=1;
select data from file(currentDatabase() || '_03762_tuple.parquet', auto, 'data JSON');

insert into function file(currentDatabase() || '_03762_map.parquet') select map('a', [], 'b', [map('c', 44, 'd', 69)]) as data settings engine_file_truncate_on_insert=1;
select data from file(currentDatabase() || '_03762_map.parquet', auto, 'data JSON');
