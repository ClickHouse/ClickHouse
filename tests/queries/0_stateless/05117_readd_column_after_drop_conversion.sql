-- Dropping a column and re-adding one of the same name but another type made every query touching that
-- column fail with 44 `ILLEGAL_COLUMN` while a part still physically carried the old column, that is until
-- the drop's mutation rewrote it. Both `ALTER` statements are metadata-only, so the part keeps the old
-- column file while the metadata already reports the new one: the read then converted the value from the
-- part's type, building `toUInt32` for a `UInt64` input and handing it the `UInt32` default of the new
-- column ("Illegal column UInt32 of first argument of function toUInt32"). A column the pending mutation
-- drops is not read from the part at all, so there is nothing to convert.

DROP TABLE IF EXISTS t_readd_wide;
CREATE TABLE t_readd_wide (id UInt64, val UInt64, c UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_readd_wide SELECT number, number, number + 100 FROM numbers(5);

-- Keep the drop's column-removal mutation from rewriting the part; the same window exists on its own
-- between the DDL and the background mutation.
SYSTEM STOP MERGES t_readd_wide;
ALTER TABLE t_readd_wide DROP COLUMN c SETTINGS alter_sync = 0;
ALTER TABLE t_readd_wide ADD COLUMN c UInt32 SETTINGS alter_sync = 0;

SELECT 'the re-added column of another type, while the part still has the old one';
SELECT count(), sum(c) FROM t_readd_wide;
SELECT * FROM t_readd_wide ORDER BY id LIMIT 1;
SELECT 'the other columns of the same part';
SELECT count(), sum(id), sum(val) FROM t_readd_wide;

SELECT 'the same answer once the mutation has rewritten the part';
SYSTEM START MERGES t_readd_wide;
ALTER TABLE t_readd_wide DELETE WHERE 0 SETTINGS mutations_sync = 2;
SELECT count(), sum(c) FROM t_readd_wide;

SELECT 're-added with the same type, with a DEFAULT expression, and as another kind of type';
-- One table per variant: a second `DROP COLUMN` of the same name is refused while the first one's
-- mutation is still pending.
DROP TABLE IF EXISTS t_readd_same_type;
CREATE TABLE t_readd_same_type (id UInt64, c UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_readd_same_type SELECT number, number + 100 FROM numbers(5);
SYSTEM STOP MERGES t_readd_same_type;
ALTER TABLE t_readd_same_type DROP COLUMN c SETTINGS alter_sync = 0;
ALTER TABLE t_readd_same_type ADD COLUMN c UInt64 SETTINGS alter_sync = 0;
SELECT groupArray(c) FROM (SELECT c FROM t_readd_same_type ORDER BY id);

DROP TABLE IF EXISTS t_readd_default;
CREATE TABLE t_readd_default (id UInt64, c UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_readd_default SELECT number, number + 100 FROM numbers(5);
SYSTEM STOP MERGES t_readd_default;
ALTER TABLE t_readd_default DROP COLUMN c SETTINGS alter_sync = 0;
ALTER TABLE t_readd_default ADD COLUMN c UInt32 DEFAULT id + 1 SETTINGS alter_sync = 0;
SELECT groupArray(c) FROM (SELECT c FROM t_readd_default ORDER BY id);

DROP TABLE IF EXISTS t_readd_string;
CREATE TABLE t_readd_string (id UInt64, c UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_readd_string SELECT number, number + 100 FROM numbers(5);
SYSTEM STOP MERGES t_readd_string;
ALTER TABLE t_readd_string DROP COLUMN c SETTINGS alter_sync = 0;
ALTER TABLE t_readd_string ADD COLUMN c String SETTINGS alter_sync = 0;
SELECT groupArray(c) FROM (SELECT c FROM t_readd_string ORDER BY id);

SELECT 'and in a compact part';
DROP TABLE IF EXISTS t_readd_compact;
CREATE TABLE t_readd_compact (id UInt64, c UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_readd_compact SELECT number, number + 100 FROM numbers(5);
SYSTEM STOP MERGES t_readd_compact;
ALTER TABLE t_readd_compact DROP COLUMN c SETTINGS alter_sync = 0;
ALTER TABLE t_readd_compact ADD COLUMN c UInt32 SETTINGS alter_sync = 0;
SELECT groupArray(c) FROM (SELECT c FROM t_readd_compact ORDER BY id);
SYSTEM START MERGES t_readd_compact;

DROP TABLE t_readd_compact;
DROP TABLE t_readd_string;
DROP TABLE t_readd_default;
DROP TABLE t_readd_same_type;
DROP TABLE t_readd_wide;
