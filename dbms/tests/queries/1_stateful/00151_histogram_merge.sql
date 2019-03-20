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
round(hst_array.1) AS start, 
round(hst_array.2) AS end,
round(hst_array.3) AS cnt
from remote('127.0.0.{1,2}', 'test', 'test_histogram')

with
histogram(3)(a) AS hst,
arrayJoin(hst) AS hst_array
select
round(hst_array.1) AS start, 
round(hst_array.2) AS end,
round(hst_array.3) AS cnt
from remote('127.0.0.{2,1}', 'test', 'test_histogram')

drop table test.test_histogram
