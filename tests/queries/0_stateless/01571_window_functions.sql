-- { echo }
-- Another test for window functions because the other one is too long.
set allow_experimental_window_functions = 1;

-- expressions in window frame
select count() over (rows between 1 + 1 preceding and 1 + 1 following) from numbers(10);

-- signed and unsigned in offset do not cause logical error
select count() over (rows between 2 following and 1 + -1 following) FROM numbers(10); -- { serverError 36 }
