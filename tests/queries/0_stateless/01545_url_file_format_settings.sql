create table file_delim(a int, b int) engine File(CSV, '01545_url_file_format_settings.csv') settings format_csv_delimiter = '|';

truncate table file_delim;

insert into file_delim select 1, 2;

-- select 1, 2 format CSV settings format_csv_delimiter='/';
create table url_delim(a int, b int) engine URL('http://127.0.0.1:8123/?query=select%201%2C%202%20format%20CSV%20settings%20format_csv_delimiter%3D%27/%27%3B%0A', CSV) settings format_csv_delimiter = '/';

select * from file_delim;

select * from url_delim;

select * from file('01545_url_file_format_settings.csv', CSV, 'a int, b int') settings format_csv_delimiter = '|';

select * from url('http://127.0.0.1:8123/?query=select%201%2C%202%20format%20CSV%20settings%20format_csv_delimiter%3D%27/%27%3B%0A', CSV, 'a int, b int') settings format_csv_delimiter = '/';

drop table file_delim;
drop table url_delim;
