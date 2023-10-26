SET max_threads = 1;
SHOW SETTING max_threads;

SET max_threads = 2;
SHOW SETTING max_threads;

SHOW SETTING `max_threads' OR name = 'max_memory_usage`;
