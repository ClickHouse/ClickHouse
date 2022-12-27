-- Tags: no-parallel

insert into function file(data_02314.csv) select number, number + 1 from numbers(5) settings engine_file_truncate_on_insert=1;
insert into function file(data_02314.csv) select number, number + 1, number + 2 from numbers(5);
desc file(data_02314.csv) settings input_format_csv_skip_first_lines=5;
select * from file(data_02314.csv) settings input_format_csv_skip_first_lines=5;

insert into function file(data_02314.tsv) select number, number + 1 from numbers(5) settings engine_file_truncate_on_insert=1;
insert into function file(data_02314.tsv) select number, number + 1, number + 2 from numbers(5);
desc file(data_02314.tsv) settings input_format_csv_skip_first_lines=5;
select * from file(data_02314.tsv) settings input_format_csv_skip_first_lines=5;

