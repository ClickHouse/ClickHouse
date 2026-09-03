-- Tags: no-fasttest, no-parallel

set flatten_nested = 1;

insert into function file(02405_data.avro) select [(1, 'aa'), (2, 'bb')]::Nested(x UInt32, y String) as nested settings engine_file_truncate_on_insert=1;
select * from file(02405_data.avro, auto, 'nested Nested(x UInt32, y String)');

insert into function file(02405_data.avro) select [(1, (2, ['aa', 'bb']), [(3, 'cc'), (4, 'dd')]), (5, (6, ['ee', 'ff']), [(7, 'gg'), (8, 'hh')])]::Nested(x UInt32, y Tuple(y1 UInt32, y2 Array(String)), z Nested(z1 UInt32, z2 String)) as nested settings engine_file_truncate_on_insert=1;
select * from file(02405_data.avro, auto, 'nested Nested(x UInt32, y Tuple(y1 UInt32, y2 Array(String)), z Nested(z1 UInt32, z2 String))');
