-- 1001 matches in one row exceeds `regexp_max_matches_per_row` (default 1000) and trips the fast-throw.
-- The low `max_memory_usage` also guards against regressing to a huge fixture that could hit MEMORY_LIMIT_EXCEEDED first.
SELECT extractAllGroupsHorizontal(materialize(repeat('a', 1001)), '(\\w)') FORMAT Null SETTINGS max_memory_usage = 100000000; -- { serverError TOO_LARGE_ARRAY_SIZE }
SELECT count(extractAllGroupsHorizontal(materialize('a'), '(a)')) FROM numbers(1000000) FORMAT Null; -- shouldn't fail
