DROP TABLE IF EXISTS dist_04201_missing_column;
DROP TABLE IF EXISTS dist_04201_bad_type;
DROP TABLE IF EXISTS dist_04201;
DROP TABLE IF EXISTS users_04201;

CREATE TABLE users_04201 (uid Int16, name String, age Int16) ENGINE = Memory;
CREATE TABLE dist_04201 ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'users_04201', cityHash64(name));

SELECT name, type
FROM system.columns
WHERE database = currentDatabase() AND table = 'dist_04201'
ORDER BY position;

INSERT INTO dist_04201 (uid, name, age) SETTINGS distributed_foreground_insert = 1 VALUES (1, 'Alice', 42);
SELECT * FROM users_04201 ORDER BY uid;

CREATE TABLE dist_04201_bad_type ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'users_04201', name); -- { serverError TYPE_MISMATCH }
CREATE TABLE dist_04201_missing_column ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'users_04201', cityHash64(missing_column)); -- { serverError UNKNOWN_IDENTIFIER }

DROP TABLE IF EXISTS dist_04201_missing_column;
DROP TABLE IF EXISTS dist_04201_bad_type;
DROP TABLE dist_04201;
DROP TABLE users_04201;
