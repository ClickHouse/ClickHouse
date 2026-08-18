-- Tags: no-parallel, no-fasttest

insert into function file(02458_data.jsonl) select NULL as x, 42 as y settings engine_file_truncate_on_insert=1;
insert into function file(02458_data.jsoncompacteachrow) select NULL as x, 42 as y settings engine_file_truncate_on_insert=1;
drop table if exists test;
create table test (x Nullable(UInt32), y UInt32) engine=Memory();

set use_structure_from_insertion_table_in_table_functions=2;
set input_format_json_infer_incomplete_types_as_strings=0;

insert into test select * from file(02458_data.jsonl);
insert into test select x, 1 from file(02458_data.jsonl);
insert into test select x, y from file(02458_data.jsonl);
insert into test select x + 1, y from file(02458_data.jsonl); -- {serverError CANNOT_EXTRACT_TABLE_STRUCTURE}
insert into test select x, z from file(02458_data.jsonl);
insert into test select * from file(02458_data.jsoncompacteachrow);
insert into test select x, 1 from file(02458_data.jsoncompacteachrow); -- {serverError CANNOT_EXTRACT_TABLE_STRUCTURE}
insert into test select x, y from file(02458_data.jsoncompacteachrow); -- {serverError CANNOT_EXTRACT_TABLE_STRUCTURE}
insert into test select x + 1, y from file(02458_data.jsoncompacteachrow); -- {serverError CANNOT_EXTRACT_TABLE_STRUCTURE}
insert into test select x, z from file(02458_data.jsoncompacteachrow); -- {serverError CANNOT_EXTRACT_TABLE_STRUCTURE}
insert into test select * from input() format CSV 1,2

insert into test select x, y from input() format CSV 1,2 -- {serverError CANNOT_EXTRACT_TABLE_STRUCTURE}

insert into test select x, y from input() format JSONEachRow {"x" : null, "y" : 42};

select * from test order by y;

drop table test;
create table test (x Nullable(UInt32)) engine=Memory();
insert into test select * from file(02458_data.jsonl);
insert into test select x from file(02458_data.jsonl);
insert into test select y from file(02458_data.jsonl);
insert into test select y as x from file(02458_data.jsonl);
insert into test select c1 from input() format CSV 1,2; -- {serverError CANNOT_EXTRACT_TABLE_STRUCTURE}

insert into test select x from input() format JSONEachRow {"x" : null, "y" : 42};

select * from test order by x;

drop table test;
