-- Tags: no-replicated-database

-- Memory limit must correctly apply, triggering an exception:

SET max_memory_usage = '100M';
SELECT length(topK(5592405)(tuple(number))) FROM numbers(10) GROUP BY number; -- { serverError 241 }
