-- Tags: no-parallel
DROP DATABASE IF EXISTS dboverlay    SYNC;
DROP DATABASE IF EXISTS db_overlay_a SYNC;
DROP DATABASE IF EXISTS db_overlay_b SYNC;

CREATE DATABASE db_overlay_a ENGINE = Atomic;
CREATE DATABASE db_overlay_b ENGINE = Atomic;

CREATE TABLE db_overlay_a.t_a
(
    id UInt32,
    s  String
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE db_overlay_b.t_b
(
    id UInt32,
    s  String
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO db_overlay_a.t_a VALUES (1, 'a1'), (2, 'a2');
INSERT INTO db_overlay_b.t_b VALUES (10, 'b10'), (20, 'b20');

CREATE DATABASE dboverlay ENGINE = Overlay('db_overlay_a', 'db_overlay_b');

SHOW CREATE DATABASE dboverlay;

SHOW TABLES FROM dboverlay;

SELECT * FROM dboverlay.t_a ORDER BY id;

SELECT * FROM dboverlay.t_b ORDER BY id;

CREATE TABLE db_overlay_a.t_new
(
    k UInt32,
    v String
)
ENGINE = MergeTree
ORDER BY k;

SHOW TABLES FROM dboverlay;

INSERT INTO db_overlay_a.t_new VALUES (100, 'x'), (200, 'y');
SELECT * FROM dboverlay.t_new ORDER BY k;

INSERT INTO dboverlay.t_new VALUES (999, 'fail'); -- { serverError TABLE_UUID_MISMATCH }

CREATE TABLE dboverlay.ct_fail (x UInt8) ENGINE = MergeTree ORDER BY x; -- { serverError BAD_ARGUMENTS }

ATTACH TABLE dboverlay.at_fail (x UInt8) ENGINE = MergeTree ORDER BY x; -- { serverError BAD_ARGUMENTS }

ALTER TABLE dboverlay.t_a ADD COLUMN z UInt8 DEFAULT 0; -- { serverError UNKNOWN_TABLE }

RENAME TABLE dboverlay.t_a TO dboverlay.t_a_renamed_via_overlay; -- { serverError BAD_ARGUMENTS }

CREATE DATABASE loop_self ENGINE = Overlay('loop_self'); -- { serverError BAD_ARGUMENTS }

CREATE DATABASE bad_overlay ENGINE = Overlay('this_db_does_not_exist'); -- { serverError BAD_ARGUMENTS }

DROP TABLE dboverlay.t_a;

RENAME TABLE db_overlay_a.t_new TO db_overlay_a.t_new_renamed;

SHOW TABLES FROM dboverlay;
SELECT * FROM dboverlay.t_new_renamed ORDER BY k;

DROP TABLE db_overlay_a.t_new_renamed;

SHOW TABLES FROM dboverlay;

DROP DATABASE dboverlay    SYNC;

SHOW TABLES FROM dboverlay; -- { serverError UNKNOWN_DATABASE }

DROP DATABASE db_overlay_a SYNC;
DROP DATABASE db_overlay_b SYNC;
