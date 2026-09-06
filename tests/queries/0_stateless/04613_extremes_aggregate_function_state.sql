-- Test for issue #100698: SELECT with `extremes = 1` over aggregate function states
-- (e.g. `argMaxState`) threw `ILLEGAL_TYPE_OF_ARGUMENT` when the result spanned
-- multiple chunks, because `ExtremesTransform` compared serialized aggregate function
-- states while merging extremes of the chunks. Extremes of columns of non-comparable
-- types now keep the default values, the same as a single-chunk result produces.

select 'multi-chunk result with aggregate function states';
select number % 100 as k, argMaxState(number, number) as st
from numbers(1000)
group by k
settings extremes = 1, max_block_size = 10, max_threads = 1
format Null;

-- Extremes of comparable columns must still be merged across chunks
select 'multi-chunk extremes of comparable columns are still merged';
select number % 10 as k, max(number) as m
from numbers(100)
group by k
order by k
settings extremes = 1, max_block_size = 3, max_threads = 1;

select 'single-chunk result still works';
select number % 2 as k, argMaxState(number, number) as st
from numbers(10)
group by k
settings extremes = 1
format Null;

select 'ok';
