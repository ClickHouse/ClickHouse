-- `compatibility` set to a release before 24.10 must keep the timeouts and Keeper retries of BACKUP/RESTORE ON CLUSTER:
-- those releases neither waited without a bound after an error nor skipped the retries.
-- `backup_restore_keeper_max_retries` shows that `compatibility` is still applied to the other settings.
SET compatibility = '22.10';
SELECT name, value FROM system.settings
WHERE name IN ('backup_restore_failure_after_host_disconnected_for_seconds',
               'backup_restore_finish_timeout_after_error_sec',
               'backup_restore_keeper_max_retries',
               'backup_restore_keeper_max_retries_while_handling_error',
               'backup_restore_keeper_max_retries_while_initializing')
ORDER BY name;
