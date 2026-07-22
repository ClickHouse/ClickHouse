-- Tags: no-fasttest, no-parallel

SET session_timezone = 'UTC';
SET engine_file_truncate_on_insert = 1;

insert into function file('03259.orc', 'ORC')
select
    number,
    if (number % 2 = 0, null, toDate32(number)) as date_field
from numbers(10);

desc file('03259.orc', 'ORC');

select date_field from file('03259.orc', 'ORC') order by number;
