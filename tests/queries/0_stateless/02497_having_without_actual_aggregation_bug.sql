SET allow_experimental_analyzer = 1;

select number from numbers_mt(10) having number >= 9;

select count() from numbers_mt(100) having count() > 1;

select queryID() as t from numbers(10) with totals having t = initialQueryID(); -- { serverError 48 }
select count() from (select queryID() as t from remote('127.0.0.{1..3}', numbers(10)) with totals having t = initialQueryID()) settings prefer_localhost_replica = 1; -- { serverError 48 }
