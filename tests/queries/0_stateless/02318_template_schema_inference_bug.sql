insert into function file(data_02318.tsv) select * from numbers(10);
desc file('data_02318.tsv', 'Template') SETTINGS format_template_row='nonexist', format_template_resultset='nonexist'; -- {serverError CANNOT_EXTRACT_TABLE_STRUCTURE}
