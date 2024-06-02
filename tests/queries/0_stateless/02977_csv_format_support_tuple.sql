-- Tags: no-parallel

insert into function file('02977_1.csv') select '20240305', 1, ['s', 'd'], map('a', 2), tuple('222', 33, map('abc', 5)) SETTINGS engine_file_truncate_on_insert=1;
desc file('02977_1.csv');
select * from file('02977_1.csv') settings max_threads=1;
