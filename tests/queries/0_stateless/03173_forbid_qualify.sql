drop table if exists test_qualify;
create table test_qualify (number Int64) ENGINE = MergeTree ORDER BY (number);

insert into test_qualify SELECT * FROM numbers(100);

select count() from test_qualify; -- 100
select * from test_qualify qualify row_number() over (order by number) = 50 SETTINGS enable_analyzer = 1; -- 49
select * from test_qualify qualify row_number() over (order by number) = 50 SETTINGS enable_analyzer = 0; -- { serverError NOT_IMPLEMENTED }

delete from test_qualify where number in (select number from test_qualify qualify row_number() over (order by number) = 50) SETTINGS validate_mutation_query = 0; -- { serverError UNFINISHED }
select count() from test_qualify; -- 100
