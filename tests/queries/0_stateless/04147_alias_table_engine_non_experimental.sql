-- Verify the `Alias` table engine no longer requires `allow_experimental_alias_table_engine`
-- and that the obsolete flag is still accepted (as a no-op) for backward compatibility.

DROP TABLE IF EXISTS alias_target;
DROP TABLE IF EXISTS alias_default;
DROP TABLE IF EXISTS alias_obsolete_flag;

CREATE TABLE alias_target (id UInt32, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO alias_target VALUES (1, 'one'), (2, 'two');

-- Without enabling the obsolete experimental setting: must succeed.
CREATE TABLE alias_default ENGINE = Alias('alias_target');
SELECT * FROM alias_default ORDER BY id;

-- The obsolete setting must still parse and be a no-op.
SET allow_experimental_alias_table_engine = 1;
CREATE TABLE alias_obsolete_flag ENGINE = Alias('alias_target');
SELECT * FROM alias_obsolete_flag ORDER BY id;

-- Setting it to 0 must also be accepted (it is no longer consulted).
SET allow_experimental_alias_table_engine = 0;
DROP TABLE alias_obsolete_flag;
CREATE TABLE alias_obsolete_flag ENGINE = Alias('alias_target');
SELECT * FROM alias_obsolete_flag ORDER BY id;

DROP TABLE alias_obsolete_flag;
DROP TABLE alias_default;
DROP TABLE alias_target;
