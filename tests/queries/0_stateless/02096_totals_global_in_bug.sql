-- Tags: x-no-randomize-result-order

select sum(number) from numbers(2) where number global in (select sum(number) from numbers(2) group by number with totals) group by number with totals

