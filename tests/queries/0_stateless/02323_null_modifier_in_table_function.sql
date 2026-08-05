select * from values('x UInt8 NOT NULL', 1);
select * from values('x UInt8 NULL', NULL);
insert into function file(currentDatabase() || '_data_02323.tsv') select number % 2 ? number : NULL from numbers(3) settings engine_file_truncate_on_insert=1;
select * from file(currentDatabase() || '_data_02323.tsv', auto, 'x UInt32 NOT NULL');
select * from file(currentDatabase() || '_data_02323.tsv', auto, 'x UInt32 NULL');
select * from generateRandom('x UInt64 NULL', 7, 3) limit 2;
