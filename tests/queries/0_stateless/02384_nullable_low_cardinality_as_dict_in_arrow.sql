-- Tags: no-fasttest

insert into function file(02384_data.arrow) select toLowCardinality(toNullable('abc')) as lc settings output_format_arrow_low_cardinality_as_dictionary=1, output_format_arrow_string_as_string=0, engine_file_truncate_on_insert=1;
desc file(02384_data.arrow);
select * from file(02384_data.arrow);
insert into function file(02384_data.arrow) select toLowCardinality(toNullable('abc')) as lc settings output_format_arrow_low_cardinality_as_dictionary=1, output_format_arrow_string_as_string=1, engine_file_truncate_on_insert=1;
desc file(02384_data.arrow);
select * from file(02384_data.arrow);
