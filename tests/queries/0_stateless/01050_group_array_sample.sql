select k, groupArraySample(10, 123456)(v) from (select number % 4 as k, number as v from numbers(1024)) group by k;

-- different seed
select k, groupArraySample(10, 1)(v) from (select number % 4 as k, number as v from numbers(1024)) group by k;
