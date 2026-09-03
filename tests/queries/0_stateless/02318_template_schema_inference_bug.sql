insert into function file(currentDatabase() || '_data_02318.tsv') select * from numbers(10);
desc file(currentDatabase() || '_data_02318.tsv', 'Template') SETTINGS format_template_row='nonexist', format_template_resultset='nonexist'; -- {serverError FILE_DOESNT_EXIST}
