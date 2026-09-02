-- the setting exists and server does not crash
SET os_threads_nice_value_query = 10;
SELECT count() FROM numbers(1000);
