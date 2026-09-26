SELECT 'Test without parameters';

DROP TABLE IF EXISTS 03409_users;

CREATE TABLE 03409_users
(
    `uid` Int16,
    `name` String,
    `age` Nullable(Int16),
    `age2` Nullable(Int16),
    `version` Nullable(UInt8)
)
ENGINE = CoalescingMergeTree()
ORDER BY (uid, name);

INSERT INTO 03409_users VALUES (111, 'John', 23, 12, 1);
INSERT INTO 03409_users VALUES (111, 'John', null, 34, null);
INSERT INTO 03409_users VALUES (111, 'John', null, null, 3);
INSERT INTO 03409_users VALUES (111, 'John', 52, null, 4);
INSERT INTO 03409_users VALUES (8888, 'Alice', 50, 50, 1);

SELECT * FROM 03409_users FINAL ORDER BY ALL;
OPTIMIZE TABLE 03409_users FINAL;
SELECT * FROM 03409_users ORDER BY ALL;

SELECT 'Test with parameters';

DROP TABLE IF EXISTS 03409_users;

CREATE TABLE 03409_users
(
    `uid` Int16,
    `name` String,
    `age` Nullable(Int16),
    `age2` Nullable(Int16),
    `version` Nullable(UInt8)
)
ENGINE = CoalescingMergeTree(version)
ORDER BY (uid, name);

INSERT INTO 03409_users VALUES (111, 'John', 23, 12, 1);
INSERT INTO 03409_users VALUES (111, 'John', null, 34, 2);
INSERT INTO 03409_users VALUES (111, 'John', null, null, null);
INSERT INTO 03409_users VALUES (111, 'John', 52, null, 4);
INSERT INTO 03409_users VALUES (8888, 'Alice', 50, 50, 1);

SELECT * FROM 03409_users FINAL ORDER BY ALL;
OPTIMIZE TABLE 03409_users FINAL;
SELECT * FROM 03409_users ORDER BY ALL;
