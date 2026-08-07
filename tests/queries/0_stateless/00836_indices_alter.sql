DROP TABLE IF EXISTS minmax_idx;
DROP TABLE IF EXISTS minmax_idx2;


CREATE TABLE minmax_idx
(
    u64 UInt64,
    i32 Int32
) ENGINE = MergeTree()
ORDER BY u64;

INSERT INTO minmax_idx VALUES (1, 2);

ALTER TABLE minmax_idx ADD INDEX idx1 u64 * i32 TYPE minmax GRANULARITY 10;
ALTER TABLE minmax_idx ADD INDEX idx2 u64 + i32 TYPE minmax GRANULARITY 10;
ALTER TABLE minmax_idx ADD INDEX idx3 (u64 - i32) TYPE minmax GRANULARITY 10 AFTER idx1;

SHOW CREATE TABLE minmax_idx;

SELECT * FROM minmax_idx WHERE u64 * i32 = 2;

INSERT INTO minmax_idx VALUES (1, 2);
INSERT INTO minmax_idx VALUES (1, 2);
INSERT INTO minmax_idx VALUES (1, 2);
INSERT INTO minmax_idx VALUES (1, 2);
INSERT INTO minmax_idx VALUES (1, 2);

SELECT * FROM minmax_idx WHERE u64 * i32 = 2;

ALTER TABLE minmax_idx DROP INDEX idx1;

SHOW CREATE TABLE minmax_idx;

SELECT * FROM minmax_idx WHERE u64 * i32 = 2;

ALTER TABLE minmax_idx DROP INDEX idx2;
ALTER TABLE minmax_idx DROP INDEX idx3;

SHOW CREATE TABLE minmax_idx;

ALTER TABLE minmax_idx ADD INDEX idx1 (u64 * i32) TYPE minmax GRANULARITY 10;

SHOW CREATE TABLE minmax_idx;

SELECT * FROM minmax_idx WHERE u64 * i32 = 2;


CREATE TABLE minmax_idx2
(
    u64 UInt64,
    i32 Int32,
    INDEX idx1 (u64 + i32) TYPE minmax GRANULARITY 10,
    INDEX idx2 u64 * i32 TYPE minmax GRANULARITY 10
) ENGINE = MergeTree()
ORDER BY u64;

INSERT INTO minmax_idx2 VALUES (1, 2);
INSERT INTO minmax_idx2 VALUES (1, 2);

SELECT * FROM minmax_idx2 WHERE u64 * i32 = 2;

ALTER TABLE minmax_idx2 DROP INDEX idx1, DROP INDEX idx2;

SHOW CREATE TABLE minmax_idx2;

SELECT * FROM minmax_idx2 WHERE u64 * i32 = 2;

DROP TABLE minmax_idx;
DROP TABLE minmax_idx2;
