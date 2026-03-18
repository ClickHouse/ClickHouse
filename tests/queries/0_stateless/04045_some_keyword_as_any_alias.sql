-- Test that SOME keyword works as an alias for ANY in quantified comparisons
-- https://github.com/ClickHouse/ClickHouse/issues/99601

-- = SOME (rewritten to IN)
SELECT 3 = SOME (SELECT number FROM numbers(1, 5));
SELECT 6 = SOME (SELECT number FROM numbers(1, 5));

-- > SOME (rewritten to > min)
SELECT 3 > SOME (SELECT number FROM numbers(1, 5));
SELECT 0 > SOME (SELECT number FROM numbers(1, 5));

-- < SOME (rewritten to < max)
SELECT 3 < SOME (SELECT number FROM numbers(1, 5));
SELECT 10 < SOME (SELECT number FROM numbers(1, 5));

-- >= SOME (rewritten to >= min)
SELECT 1 >= SOME (SELECT number FROM numbers(1, 5));
SELECT 0 >= SOME (SELECT number FROM numbers(1, 5));

-- <= SOME (rewritten to <= max)
SELECT 5 <= SOME (SELECT number FROM numbers(1, 5));
SELECT 6 <= SOME (SELECT number FROM numbers(1, 5));

-- Verify SOME produces identical results to ANY
SELECT (3 = SOME (SELECT number FROM numbers(1, 5))) = (3 = ANY (SELECT number FROM numbers(1, 5)));
SELECT (3 > SOME (SELECT number FROM numbers(1, 5))) = (3 > ANY (SELECT number FROM numbers(1, 5)));
SELECT (3 < SOME (SELECT number FROM numbers(1, 5))) = (3 < ANY (SELECT number FROM numbers(1, 5)));
