-- Tags: no-parallel

select number as x, number % 3 as y, 'Hello' as z from numbers(5) format SQLInsert;
select number as x, number % 3 as y, 'Hello' as z from numbers(5) format SQLInsert settings output_format_sql_insert_max_batch_size=1;
select number as x, number % 3 as y, 'Hello' as z from numbers(5) format SQLInsert settings output_format_sql_insert_max_batch_size=2;
select number as x, number % 3 as y, 'Hello' as z from numbers(5) format SQLInsert settings output_format_sql_insert_include_column_names=0;
select number as x, number % 3 as y, 'Hello' as z from numbers(5) format SQLInsert settings output_format_sql_insert_use_replace=1;
select number as x, number % 3 as y, 'Hello' as z from numbers(5) format SQLInsert settings output_format_sql_insert_table_name='test';
select number as x, number % 3 as y, 'Hello' as z from numbers(5) format SQLInsert settings output_format_sql_insert_table_name='test', output_format_sql_insert_quote_names=0;
insert into function file(02322_data.sql, 'SQLInsert') select number as x, number % 3 as y, 'Hello' as z from numbers(5) settings output_format_sql_insert_max_batch_size=2, output_format_sql_insert_quote_names=0, engine_file_truncate_on_insert=1;
select * from file(02322_data.sql, 'MySQLDump');
insert into function file(02322_data.sql, 'SQLInsert') select number, number % 3, 'Hello' from numbers(5) settings output_format_sql_insert_max_batch_size=2, engine_file_truncate_on_insert=1;
select * from file(02322_data.sql, 'MySQLDump');
