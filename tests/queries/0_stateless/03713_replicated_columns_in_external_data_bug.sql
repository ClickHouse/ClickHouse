set enable_analyzer=1;
select * from remote('127.0.0.{1,2,3}', numbers(100)) where number global in (select number::Dynamic from numbers(100) array Join range(number % 10)) limit 100 format Null settings enable_lazy_columns_replication=1;

