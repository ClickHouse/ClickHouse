drop table if exists default.test_qualify;
create table default.test_qualify (number Int64) ENGINE = MergeTree ORDER BY (number);

insert into default.test_qualify SELECT * FROM numbers(100);

select count() from default.test_qualify; -- 100
select * from default.test_qualify qualify row_number() over (order by number) = 50 SETTINGS allow_experimental_analyzer = 1; -- 49
select * from default.test_qualify qualify row_number() over (order by number) = 50 SETTINGS allow_experimental_analyzer = 0; -- { serverError NOT_IMPLEMENTED }

delete from default.test_qualify where number in (select number from default.test_qualify qualify row_number() over (order by number) = 50); -- { serverError UNFINISHED }
select count() from default.test_qualify; -- 100
