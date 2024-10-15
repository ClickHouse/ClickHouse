DROP TABLE IF EXISTS table_with_version;

CREATE TABLE table_with_version
(
    key UInt64,
    value String,
    version UInt8,
    sign Int8
)
ENGINE VersionedCollapsingMergeTree(sign, version)
ORDER BY key;

INSERT INTO table_with_version VALUES (1, '1', 1, -1);
INSERT INTO table_with_version VALUES (2, '2', 2, -1);

SELECT * FROM table_with_version ORDER BY key;

SHOW CREATE TABLE table_with_version;

ALTER TABLE table_with_version MODIFY COLUMN version UInt32;

SELECT * FROM table_with_version ORDER BY key;

SHOW CREATE TABLE table_with_version;

INSERT INTO TABLE table_with_version VALUES(1, '1', 1, 1);
INSERT INTO TABLE table_with_version VALUES(1, '1', 2, 1);

SELECT * FROM table_with_version FINAL ORDER BY key;

INSERT INTO TABLE table_with_version VALUES(3, '3', 65555, 1);

SELECT * FROM table_with_version FINAL ORDER BY key;

INSERT INTO TABLE table_with_version VALUES(3, '3', 65555, -1);

SELECT * FROM table_with_version FINAL ORDER BY key;

ALTER TABLE table_with_version MODIFY COLUMN version String; --{serverError ALTER_OF_COLUMN_IS_FORBIDDEN}
ALTER TABLE table_with_version MODIFY COLUMN version Int64; --{serverError ALTER_OF_COLUMN_IS_FORBIDDEN}
ALTER TABLE table_with_version MODIFY COLUMN version UInt16; --{serverError ALTER_OF_COLUMN_IS_FORBIDDEN}
ALTER TABLE table_with_version MODIFY COLUMN version Float64; --{serverError ALTER_OF_COLUMN_IS_FORBIDDEN}
ALTER TABLE table_with_version MODIFY COLUMN version Date; --{serverError ALTER_OF_COLUMN_IS_FORBIDDEN}
ALTER TABLE table_with_version MODIFY COLUMN version DateTime; --{serverError ALTER_OF_COLUMN_IS_FORBIDDEN}

DROP TABLE IF EXISTS table_with_version;
