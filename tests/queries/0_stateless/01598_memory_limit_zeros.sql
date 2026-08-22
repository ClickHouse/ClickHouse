-- Tags: no-parallel, no-fasttest, no-random-settings

SET max_memory_usage = 1, max_untracked_memory = 1000000, max_threads=40;
select 'test', count(*) from zeros_mt(1000000) where not ignore(zero); -- { serverError MEMORY_LIMIT_EXCEEDED }
