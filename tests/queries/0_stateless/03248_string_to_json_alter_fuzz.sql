set allow_experimental_json_type=1;
set max_insert_block_size=10000;
set max_block_size=10000;

drop table if exists test;
drop named collection if exists json_alter_fuzzer;

create table test (json String) engine=MergeTree order by tuple();
create named collection json_alter_fuzzer AS json_str='{}';
insert into test select * from fuzzJSON(json_alter_fuzzer, reuse_output=true, max_output_length=64) limit 200000;
alter table test modify column json JSON(max_dynamic_paths=100) settings mutations_sync=1;
select json from test format Null;
optimize table test final;
select json from test format Null;
drop named collection json_alter_fuzzer;
drop table test;

