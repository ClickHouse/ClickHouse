-- Tags: replica, no-shared-merge-tree, no-distributed-cache
-- Tag no-shared-merge-tree: Requires update in private (@jkartseva)
-- Tag no-distributed-cache: Requires update in private (@jkartseva)

SELECT 'Test without parameters';

DROP TABLE IF EXISTS 03409_users SYNC;

CREATE TABLE 03409_users
(
    `uid` Int16,
    `name` String,
    `age` Nullable(Int16),
    `age2` Nullable(Int16),
    `version` Nullable(UInt8)
)
ENGINE = ReplicatedCoalescingMergeTree('/clickhouse/tables/{database}/test_00754/summing', 'r1')
ORDER BY (uid, name);

INSERT INTO 03409_users VALUES (111, 'John', 23, 12, 1);
INSERT INTO 03409_users VALUES (111, 'John', null, 34, null);
INSERT INTO 03409_users VALUES (111, 'John', null, null, 3);
INSERT INTO 03409_users VALUES (111, 'John', 52, null, 4);
INSERT INTO 03409_users VALUES (8888, 'Alice', 50, 50, 1);

OPTIMIZE TABLE 03409_users FINAL;
SYSTEM SYNC REPLICA 03409_users;

SELECT * FROM 03409_users ORDER BY ALL;

SELECT 'Test with parameters';

DROP TABLE IF EXISTS 03409_users SYNC;

CREATE TABLE 03409_users
(
    `uid` Int16,
    `name` String,
    `age` Nullable(Int16),
    `age2` Nullable(Int16),
    `version` Nullable(UInt8)
)
ENGINE = ReplicatedCoalescingMergeTree('/clickhouse/tables/{database}/test_00754/summing', 'r1', version)
ORDER BY (uid, name);

INSERT INTO 03409_users VALUES (111, 'John', 23, 12, 1);
INSERT INTO 03409_users VALUES (111, 'John', null, 34, 2);
INSERT INTO 03409_users VALUES (111, 'John', null, null, null);
INSERT INTO 03409_users VALUES (111, 'John', 52, null, 4);
INSERT INTO 03409_users VALUES (8888, 'Alice', 50, 50, 1);

OPTIMIZE TABLE 03409_users FINAL;
SYSTEM SYNC REPLICA 03409_users;

SELECT * FROM 03409_users ORDER BY ALL;
