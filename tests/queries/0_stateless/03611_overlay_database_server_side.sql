-- { echo }
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE IF EXISTS db_overlay_a;
DROP DATABASE IF EXISTS db_overlay_b;

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

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Overlay('db_overlay_a', 'db_overlay_b');

SHOW CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

SHOW TABLES FROM {CLICKHOUSE_DATABASE_1:Identifier};

SELECT * FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_a ORDER BY id;

SELECT * FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_b ORDER BY id;

CREATE TABLE db_overlay_a.t_new
(
    k UInt32,
    v String
)
ENGINE = MergeTree
ORDER BY k;

SHOW TABLES FROM {CLICKHOUSE_DATABASE_1:Identifier};

INSERT INTO db_overlay_a.t_new VALUES (100, 'x'), (200, 'y');
SELECT * FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_new ORDER BY k;

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_new VALUES (999, 'Pass-through overlay');

ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_a ADD COLUMN z UInt8 DEFAULT 0;

SELECT * FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_new ORDER BY k;

SELECT * FROM db_overlay_a.t_new ORDER BY k;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.ct_fail (x UInt8) ENGINE = MergeTree ORDER BY x; -- { serverError BAD_ARGUMENTS }

ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.at_fail (x UInt8) ENGINE = MergeTree ORDER BY x; -- { serverError BAD_ARGUMENTS }

RENAME TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_a TO {CLICKHOUSE_DATABASE_1:Identifier}.t_a_renamed_via_overlay; -- { serverError BAD_ARGUMENTS }

CREATE DATABASE loop_self ENGINE = Overlay('loop_self'); -- { serverError BAD_ARGUMENTS }

CREATE DATABASE bad_overlay ENGINE = Overlay('this_db_does_not_exist'); -- { serverError BAD_ARGUMENTS }

DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_a;

RENAME TABLE db_overlay_a.t_new TO db_overlay_a.t_new_renamed;

SHOW TABLES FROM {CLICKHOUSE_DATABASE_1:Identifier};
SELECT * FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_new_renamed ORDER BY k;

DROP TABLE db_overlay_a.t_new_renamed;

SHOW TABLES FROM {CLICKHOUSE_DATABASE_1:Identifier};

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

SHOW TABLES FROM {CLICKHOUSE_DATABASE_1:Identifier}; -- { serverError UNKNOWN_DATABASE }

DROP DATABASE db_overlay_a;
DROP DATABASE db_overlay_b;
