-- { echo }
-- Another test for window functions because the other one is too long.
set allow_experimental_window_functions = 1;

-- expressions in window frame
select count() over (rows between 1 + 1 preceding and 1 + 1 following) from numbers(10);
