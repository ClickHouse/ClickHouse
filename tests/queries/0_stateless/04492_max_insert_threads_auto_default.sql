-- Since 26.7, `max_insert_threads` defaults to auto (the number of CPU cores) instead of 1,
-- so `INSERT SELECT` is parallelized by default.

-- The default is `auto(N)`; the exact N depends on the machine, so only check the `auto` marker.
SELECT default LIKE '%auto(%' FROM system.settings WHERE name = 'max_insert_threads';

-- An explicit 0 also means auto (consistent with `max_threads`).
SELECT value LIKE '%auto(%' FROM system.settings WHERE name = 'max_insert_threads' SETTINGS max_insert_threads = 0;

-- Explicitly setting it to 1 disables parallel `INSERT SELECT`.
SELECT value FROM system.settings WHERE name = 'max_insert_threads' SETTINGS max_insert_threads = 1;

-- The `compatibility` setting restores the previous default of 1 (single-threaded).
SELECT value FROM system.settings WHERE name = 'max_insert_threads' SETTINGS compatibility = '26.6';
