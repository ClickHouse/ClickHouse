-- Tags: no-parallel
-- { echo }
DROP DATABASE IF EXISTS db_overlay;
DROP DATABASE IF EXISTS db_a;
DROP DATABASE IF EXISTS db_b;

CREATE DATABASE db_a ENGINE = Atomic;
CREATE DATABASE db_b ENGINE = Atomic;

CREATE TABLE db_a.t_a
(
    id UInt32,
    s  String
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE db_b.t_b
(
    id UInt32,
    s  String
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO db_a.t_a VALUES (1, 'a1'), (2, 'a2');
INSERT INTO db_b.t_b VALUES (10, 'b10'), (20, 'b20');

CREATE DATABASE db_overlay ENGINE = Overlay('db_a', 'db_b');

SHOW CREATE DATABASE db_overlay;

SHOW TABLES FROM db_overlay;

SELECT * FROM db_overlay.t_a ORDER BY id;

SELECT * FROM db_overlay.t_b ORDER BY id;

CREATE TABLE db_a.t_new
(
    k UInt32,
    v String
)
ENGINE = MergeTree
ORDER BY k;

SHOW TABLES FROM db_overlay;

INSERT INTO db_a.t_new VALUES (100, 'x'), (200, 'y');
SELECT * FROM db_overlay.t_new ORDER BY k;

INSERT INTO db_overlay.t_new VALUES (999, 'Pass-through overlay');

ALTER TABLE db_overlay.t_a ADD COLUMN z UInt8 DEFAULT 0; -- { serverError TABLE_IS_READ_ONLY }

SELECT * FROM db_overlay.t_new ORDER BY k;

SELECT * FROM db_a.t_new ORDER BY k;

CREATE TABLE db_overlay.ct_fail (x UInt8) ENGINE = MergeTree ORDER BY x; -- { serverError BAD_ARGUMENTS }

ATTACH TABLE db_overlay.at_fail (x UInt8) ENGINE = MergeTree ORDER BY x; -- { serverError BAD_ARGUMENTS }

RENAME TABLE db_overlay.t_a TO db_overlay.t_a_renamed_via_overlay; -- { serverError BAD_ARGUMENTS }

CREATE DATABASE loop_self ENGINE = Overlay('loop_self'); -- { serverError BAD_ARGUMENTS }

CREATE DATABASE bad_overlay ENGINE = Overlay('this_db_does_not_exist'); -- { serverError BAD_ARGUMENTS }

-- DROP/DETACH/TRUNCATE TABLE on the facade are rejected so that the underlying tables are not stopped behind the user's back.
DROP TABLE db_overlay.t_a; -- { serverError BAD_ARGUMENTS }
DETACH TABLE db_overlay.t_a; -- { serverError BAD_ARGUMENTS }
TRUNCATE TABLE db_overlay.t_a; -- { serverError TABLE_IS_READ_ONLY }

-- The rejected operations had no side effect on the underlying table: it is still fully operational.
INSERT INTO db_a.t_a VALUES (3, 'a3');
SELECT * FROM db_overlay.t_a ORDER BY id;

-- A reference cycle formed by re-creating a source database is detected instead of exhausting the stack.
DROP DATABASE IF EXISTS cyc_a;
DROP DATABASE IF EXISTS cyc_b;
CREATE DATABASE cyc_a ENGINE = Atomic;
CREATE DATABASE cyc_b ENGINE = Overlay('cyc_a');
DROP DATABASE cyc_a;
CREATE DATABASE cyc_a ENGINE = Overlay('cyc_b');
SHOW TABLES FROM cyc_a; -- { serverError BAD_ARGUMENTS }
SELECT * FROM cyc_b.t; -- { serverError BAD_ARGUMENTS }
DROP DATABASE cyc_a;
DROP DATABASE cyc_b;

-- BACKUP of the facade stores only its definition; the tables belong to (and are backed up with) the underlying databases.
BACKUP DATABASE db_overlay TO Memory('03611_overlay_backup') FORMAT Null;
DROP DATABASE db_overlay;
RESTORE DATABASE db_overlay FROM Memory('03611_overlay_backup') FORMAT Null;
SHOW TABLES FROM db_overlay;
SELECT * FROM db_overlay.t_a ORDER BY id;

RENAME TABLE db_a.t_new TO db_a.t_new_renamed;

SHOW TABLES FROM db_overlay;
SELECT * FROM db_overlay.t_new_renamed ORDER BY k;

DROP TABLE db_a.t_new_renamed;

SHOW TABLES FROM db_overlay;

DROP DATABASE db_overlay;

SHOW TABLES FROM db_overlay; -- { serverError UNKNOWN_DATABASE }

DROP DATABASE db_a;
DROP DATABASE db_b;
