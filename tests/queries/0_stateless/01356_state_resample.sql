select sumResample(0, 20, 1)(number, number % 20) from numbers(200);
select arrayMap(x -> finalizeAggregation(x), state) from (select sumStateResample(0, 20, 1)(number, number % 20) as state from numbers(200));
select arrayMap(x -> finalizeAggregation(x), state) from
(
    select sumStateResample(0,20,1)(number, number%20) as state from numbers(200) group by number % 3 order by number % 3
);

select groupArrayResample(0, 20, 1)(number, number % 20) from numbers(50);
select arrayMap(x -> finalizeAggregation(x), state) from (select groupArrayStateResample(0, 20, 1)(number, number % 20) state from numbers(50));

select arrayMap(x -> finalizeAggregation(x), state) from 
(
    select sumStateResample(0, 20, 1)(number, number % 20) as state from remote('127.0.0.{1,2}', numbers(200))
);
