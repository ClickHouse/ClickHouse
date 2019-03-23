create table test.test_histogram (a Int32) 
ENGINE = Memory

--127.0.0.1
insert into test.test_histogram (a) SELECT * FROM numbers(10, 190)

--127.0.0.2
insert into test.test_histogram (a) SELECT * FROM numbers(0, 100)


--make sure upper_bound and lower_bound are correct 
--when merging AggregationState in both 1,2 and 2,1 order

with
histogram(3)(a) AS hst,
arrayJoin(hst) AS hst_array
select
round(hst_array.2) AS upper_bound
from remote('127.0.0.{1,2}', 'test', 'test_histogram')
having upper_bound = 199
order by upper_bound desc
limit 1

with
histogram(3)(a) AS hst,
arrayJoin(hst) AS hst_array
select
round(hst_array.2) AS upper_bound
from remote('127.0.0.{2,1}', 'test', 'test_histogram')
having upper_bound = 199
order by upper_bound desc
limit 1

drop table test.test_histogram
