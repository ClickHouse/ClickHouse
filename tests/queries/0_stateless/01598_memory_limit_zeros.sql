SET max_memory_usage = 1;
select 'test', count(*) from zeros_mt(1000000) where not ignore(zero); -- { serverError 241 }
