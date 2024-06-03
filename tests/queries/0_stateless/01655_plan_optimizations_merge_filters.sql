set allow_experimental_analyzer=1;
select explain from (explain actions = 1 select * from (select sum(number) as v, bitAnd(number, 15) as key from numbers(1e8) group by key having v != 0) where key = 7) where explain like '%Filter%' or explain like '%Aggregating%';

set allow_experimental_analyzer=0;
select explain from (explain actions = 1 select * from (select sum(number) as v, bitAnd(number, 15) as key from numbers(1e8) group by key having v != 0) where key = 7) where explain like '%Filter%' or explain like '%Aggregating%';
