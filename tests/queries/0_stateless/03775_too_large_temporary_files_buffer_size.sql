-- A `temporary_files_buffer_size` above 1 GiB is clamped by `doSettingsSanityCheckClamp`, not
-- rejected. It used to be rejected in `ProcessList::insert`, which runs for every query, so the
-- value was accepted by `SET` and then failed every following statement - including the `SET`
-- that would put it back, leaving the session unusable.
-- The clamp logs a warning, which would otherwise reach the client's stderr and fail the test.
SET send_logs_level = 'fatal';
SET temporary_files_buffer_size = 9223372036854775806;
SELECT value FROM system.settings WHERE name = 'temporary_files_buffer_size';
SELECT 'session alive';

SET temporary_files_buffer_size = 1048576;
SELECT value FROM system.settings WHERE name = 'temporary_files_buffer_size';

-- Spilling to temporary files still works.
SELECT count() FROM (SELECT number FROM numbers(1000) GROUP BY number
    SETTINGS max_bytes_before_external_group_by = 1, group_by_two_level_threshold = 1);
