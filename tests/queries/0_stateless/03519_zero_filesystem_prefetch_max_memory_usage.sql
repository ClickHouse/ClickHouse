-- Checks that session setting 'filesystem_prefetch_max_memory_usage' must not be 0

SET filesystem_prefetch_max_memory_usage = 0; -- { serverError BAD_ARGUMENTS }
