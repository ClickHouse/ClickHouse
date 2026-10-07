-- Tags: no-parallel
-- The failpoint applies to settings changes across all tables.
-- It fires once and disarms itself, but it is disarmed explicitly too: a fail point is server-global
-- state and must not stay armed if the `ALTER` fails for another reason.

CREATE TABLE readonly_settings_rollback (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS table_readonly = 0;

SYSTEM ENABLE FAILPOINT mt_alter_settings_throw_before_metadata_commit;
ALTER TABLE readonly_settings_rollback MODIFY SETTING table_readonly = 1; -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT mt_alter_settings_throw_before_metadata_commit;
INSERT INTO readonly_settings_rollback VALUES (1);
SELECT count() FROM readonly_settings_rollback;

ALTER TABLE readonly_settings_rollback MODIFY SETTING table_readonly = 1;
SYSTEM ENABLE FAILPOINT mt_alter_settings_throw_before_metadata_commit;
ALTER TABLE readonly_settings_rollback MODIFY SETTING table_readonly = 0; -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT mt_alter_settings_throw_before_metadata_commit;
INSERT INTO readonly_settings_rollback VALUES (2); -- { serverError TABLE_IS_PERMANENTLY_READ_ONLY }

DETACH TABLE readonly_settings_rollback;
ATTACH TABLE readonly_settings_rollback;
INSERT INTO readonly_settings_rollback VALUES (2); -- { serverError TABLE_IS_PERMANENTLY_READ_ONLY }
ALTER TABLE readonly_settings_rollback MODIFY SETTING table_readonly = 0;
INSERT INTO readonly_settings_rollback VALUES (2);
SELECT count() FROM readonly_settings_rollback;
DROP TABLE readonly_settings_rollback SYNC;
