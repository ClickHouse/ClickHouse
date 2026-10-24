-- Negative zero is canonicalized to positive zero in hash tables, so both of them are counted
-- as a single value, and the size of the state does not depend on the random numbers.
WITH number % 1000 = 0 ? (rand() % 2 ? 0.0 : -0.0) : number::Float64 AS x SELECT length(uniqExactState(x)::String) FROM numbers(1000000);
