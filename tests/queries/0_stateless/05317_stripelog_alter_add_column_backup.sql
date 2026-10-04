-- Backup and restore of a `StripeLog` table whose blocks were written with different schemas after `ADD COLUMN`.

DROP TABLE IF EXISTS stripelog_backup_src;
DROP TABLE IF EXISTS stripelog_backup_dst;

CREATE TABLE stripelog_backup_src (a UInt64) ENGINE = StripeLog;
INSERT INTO stripelog_backup_src VALUES (1);
ALTER TABLE stripelog_backup_src ADD COLUMN b UInt64 DEFAULT a + 10;
INSERT INTO stripelog_backup_src (a) VALUES (2);
ALTER TABLE stripelog_backup_src ADD COLUMN c UInt64 DEFAULT b * 2;
INSERT INTO stripelog_backup_src (a) VALUES (3);
INSERT INTO stripelog_backup_src VALUES (4, 100, 200);

BACKUP TABLE stripelog_backup_src TO Memory('05317_stripelog_backup') FORMAT Null;

SELECT 'restore into a new table';
RESTORE TABLE stripelog_backup_src AS stripelog_backup_dst FROM Memory('05317_stripelog_backup') FORMAT Null;
SELECT * FROM stripelog_backup_dst ORDER BY a;

SELECT 'restore into a non-empty table';
INSERT INTO stripelog_backup_dst VALUES (5, 50, 500);
RESTORE TABLE stripelog_backup_src AS stripelog_backup_dst FROM Memory('05317_stripelog_backup')
    SETTINGS allow_non_empty_tables = 1 FORMAT Null;
SELECT * FROM stripelog_backup_dst ORDER BY a, b;
DETACH TABLE stripelog_backup_dst;
ATTACH TABLE stripelog_backup_dst;
SELECT count(), sum(a), sum(b), sum(c) FROM stripelog_backup_dst;

SELECT 'restore into a table with a different default';
DROP TABLE stripelog_backup_dst;
CREATE TABLE stripelog_backup_dst (a UInt64, b UInt64 DEFAULT a + 20, c UInt64 DEFAULT b * 2) ENGINE = StripeLog;
RESTORE TABLE stripelog_backup_src AS stripelog_backup_dst FROM Memory('05317_stripelog_backup') FORMAT Null; -- { serverError CANNOT_RESTORE_TABLE }
SELECT count() FROM stripelog_backup_dst;

DROP TABLE stripelog_backup_src;
DROP TABLE stripelog_backup_dst;
