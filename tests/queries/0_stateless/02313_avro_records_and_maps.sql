-- Tags: no-fasttest

insert into function file(currentDatabase() || '_data_02313.avro') select tuple(number, 'String')::Tuple(a UInt32, b String) as t from numbers(3) settings engine_file_truncate_on_insert=1;
desc file(currentDatabase() || '_data_02313.avro');
select * from file(currentDatabase() || '_data_02313.avro');

insert into function file(currentDatabase() || '_data_02313.avro') select tuple(number, tuple(number + 1, number + 2), range(number))::Tuple(a UInt32, b Tuple(c UInt32, d UInt32), e Array(UInt32)) as t from numbers(3) settings engine_file_truncate_on_insert=1;
desc file(currentDatabase() || '_data_02313.avro');
select * from file(currentDatabase() || '_data_02313.avro');

insert into function file(currentDatabase() || '_data_02313.avro', auto, 'a Nested(b UInt32, c UInt32)') select [number, number + 1], [number + 2, number + 3] from  numbers(3) settings engine_file_truncate_on_insert=1;
desc file(currentDatabase() || '_data_02313.avro');
select * from file(currentDatabase() || '_data_02313.avro');

insert into function file(currentDatabase() || '_data_02313.avro', auto, 'a Nested(b Nested(c UInt32, d UInt32))') select [[(number, number + 1), (number + 2, number + 3)]] from  numbers(3) settings engine_file_truncate_on_insert=1;
desc file(currentDatabase() || '_data_02313.avro');
select * from file(currentDatabase() || '_data_02313.avro');

insert into function file(currentDatabase() || '_data_02313.avro') select map(concat('key_', toString(number)), number) as m from numbers(3) settings engine_file_truncate_on_insert=1;
desc file(currentDatabase() || '_data_02313.avro');
select * from file(currentDatabase() || '_data_02313.avro');

insert into function file(currentDatabase() || '_data_02313.avro') select map(concat('key_', toString(number)), tuple(number, range(number))) as m from numbers(3) settings engine_file_truncate_on_insert=1;
desc file(currentDatabase() || '_data_02313.avro');
select * from file(currentDatabase() || '_data_02313.avro');
