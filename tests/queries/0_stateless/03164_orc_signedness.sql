-- Tags: no-fasttest

set input_format_orc_filter_push_down = 1;
set engine_file_truncate_on_insert = 1;

insert into function file(currentDatabase() || 'i8.orc') select materialize(-128)::Int8 as x;
insert into function file(currentDatabase() || 'u8.orc') select materialize(128)::UInt8 as x;
insert into function file(currentDatabase() || 'i16.orc') select materialize(-32768)::Int16 as x;
insert into function file(currentDatabase() || 'u16.orc') select materialize(32768)::UInt16 as x;
insert into function file(currentDatabase() || 'i32.orc') select materialize(-2147483648)::Int32 as x;
insert into function file(currentDatabase() || 'u32.orc') select materialize(2147483648)::UInt32 as x;
insert into function file(currentDatabase() || 'i64.orc') select materialize(-9223372036854775808)::Int64 as x;
insert into function file(currentDatabase() || 'u64.orc') select materialize(9223372036854775808)::UInt64 as x;

-- { echoOn }
select x from file(currentDatabase() || 'i8.orc') where indexHint(x = -128);
select x from file(currentDatabase() || 'i8.orc') where indexHint(x = 128);
select x from file(currentDatabase() || 'u8.orc') where indexHint(x = -128);
select x from file(currentDatabase() || 'u8.orc') where indexHint(x = 128);

select x from file(currentDatabase() || 'i16.orc') where indexHint(x = -32768);
select x from file(currentDatabase() || 'i16.orc') where indexHint(x = 32768);
select x from file(currentDatabase() || 'u16.orc') where indexHint(x = -32768);
select x from file(currentDatabase() || 'u16.orc') where indexHint(x = 32768);

select x from file(currentDatabase() || 'i32.orc') where indexHint(x = -2147483648);
select x from file(currentDatabase() || 'i32.orc') where indexHint(x = 2147483648);
select x from file(currentDatabase() || 'u32.orc') where indexHint(x = -2147483648);
select x from file(currentDatabase() || 'u32.orc') where indexHint(x = 2147483648);

select x from file(currentDatabase() || 'i64.orc') where indexHint(x = -9223372036854775808);
select x from file(currentDatabase() || 'i64.orc') where indexHint(x = 9223372036854775808);
select x from file(currentDatabase() || 'u64.orc') where indexHint(x = -9223372036854775808);
select x from file(currentDatabase() || 'u64.orc') where indexHint(x = 9223372036854775808);

select x from file(currentDatabase() || 'u8.orc', ORC, 'x UInt8') where indexHint(x > 10);
select x from file(currentDatabase() || 'u8.orc', ORC, 'x UInt64') where indexHint(x > 10);
select x from file(currentDatabase() || 'u16.orc', ORC, 'x UInt16') where indexHint(x > 10);
select x from file(currentDatabase() || 'u16.orc', ORC, 'x UInt64') where indexHint(x > 10);
select x from file(currentDatabase() || 'u32.orc', ORC, 'x UInt32') where indexHint(x > 10);
select x from file(currentDatabase() || 'u32.orc', ORC, 'x UInt64') where indexHint(x > 10);
select x from file(currentDatabase() || 'u64.orc', ORC, 'x UInt64') where indexHint(x > 10);
