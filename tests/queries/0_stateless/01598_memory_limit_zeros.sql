SET max_memory_usage = 1, max_untracked_memory = 1000000;
select 'test', count(*) from zeros_mt(1000000) where not ignore(zero); -- { serverError 241 }
