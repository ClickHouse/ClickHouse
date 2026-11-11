-- Tags: no-parallel
insert into function file(02376_data.arrow) select toLowCardinality(toString(number)) as x from numbers(10) settings output_format_arrow_string_as_string=1, output_format_arrow_low_cardinality_as_dictionary=1, engine_file_truncate_on_insert=1;
desc file (02376_data.arrow);
select * from file(02376_data.arrow);
