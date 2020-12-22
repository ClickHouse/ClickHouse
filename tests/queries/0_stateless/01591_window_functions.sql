-- { echo }

set allow_experimental_window_functions = 1;

-- just something basic
select number, count() over (partition by intDiv(number, 3) order by number) from numbers(10);

-- proper calculation across blocks
select number, max(number) over (partition by intDiv(number, 3) order by number desc) from numbers(10) settings max_block_size = 2;

-- not a window function
select number, abs(number) over (partition by toString(intDiv(number, 3))) from numbers(10); -- { serverError 63 }

-- no partition by
select number, avg(number) over (order by number) from numbers(10);

-- no order by
select number, quantileExact(number) over (partition by intDiv(number, 3)) from numbers(10);

-- can add an alias after window spec
select number, quantileExact(number) over (partition by intDiv(number, 3)) q from numbers(10);

-- can't reference it yet -- the window functions are calculated at the
-- last stage of select, after all other functions.
select q * 10, quantileExact(number) over (partition by intDiv(number, 3)) q from numbers(10); -- { serverError 47 }

-- should work in ORDER BY though
select number, max(number) over (partition by intDiv(number, 3) order by number desc) m from numbers(10) order by m desc, number;

-- this one doesn't work yet -- looks like the column names clash, and the
-- window count() is overwritten with aggregate count()
-- select number, count(), count() over (partition by intDiv(number, 3)) from numbers(10) group by number order by count() desc;

-- different windows
-- an explain test would also be helpful, but it's too immature now and I don't
-- want to change reference all the time
select number, max(number) over (partition by intDiv(number, 3) order by number desc), count(number) over (partition by intDiv(number, 5) order by number) as m from numbers(31) order by number settings max_block_size = 2;

-- two functions over the same window
-- an explain test would also be helpful, but it's too immature now and I don't
-- want to change reference all the time
select number, max(number) over (partition by intDiv(number, 3) order by number desc), count(number) over (partition by intDiv(number, 3) order by number desc) as m from numbers(7) order by number settings max_block_size = 2;
