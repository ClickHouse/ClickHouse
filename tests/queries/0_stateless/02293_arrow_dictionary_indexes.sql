-- Tags: no-fasttest
insert into function file(02293_data.arrow) select toLowCardinality(toString(number)) from numbers(300) settings output_format_arrow_low_cardinality_as_dictionary=1, engine_file_truncate_on_insert=1;
select * from file(02293_data.arrow);
