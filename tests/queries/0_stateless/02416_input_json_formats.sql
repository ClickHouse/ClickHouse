-- Tags: no-parallel, no-fasttest
insert into function file(data_02416.json) select number::UInt32 as n, 'Hello' as s, range(number) as a from numbers(3) settings engine_file_truncate_on_insert=1;
desc file(data_02416.json);
select * from file(data_02416.json);

insert into function file(data_02416.jsonCompact) select number::UInt32 as n, 'Hello' as s, range(number) as a from numbers(3) settings engine_file_truncate_on_insert=1;
desc file(data_02416.jsonCompact);
select * from file(data_02416.jsonCompact);

insert into function file(data_02416.jsonColumnsWithMetadata) select number::UInt32 as n, 'Hello' as s, range(number) as a from numbers(3) settings engine_file_truncate_on_insert=1;
desc file(data_02416.jsonColumnsWithMetadata);
select * from file(data_02416.jsonColumnsWithMetadata);

