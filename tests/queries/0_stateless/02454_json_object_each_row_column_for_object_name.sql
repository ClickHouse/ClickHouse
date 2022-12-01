-- Tags: no-fasttest, no-parallel
set format_json_object_each_row_column_for_object_name='name';

select number, concat('name_', toString(number)) as name from numbers(3) format JSONObjectEachRow;
select number, concat('name_', toString(number)) as name, number + 1 as x from numbers(3) format JSONObjectEachRow;
select concat('name_', toString(number)) as name, number from numbers(3) format JSONObjectEachRow;

insert into function file(02454_data.jsonobjecteachrow) select number, concat('name_', toString(number)) as name from numbers(3) settings engine_file_truncate_on_insert=1;
desc file(02454_data.jsonobjecteachrow);
select * from file(02454_data.jsonobjecteachrow);

