-- Reproducer from https://github.com/ClickHouse/ClickHouse/issues/63460
DROP TABLE IF EXISTS 03164_users;
CREATE TABLE 03164_users (uid Nullable(Int16), name String, age Int16) ENGINE=MergeTree ORDER BY (uid) SETTINGS allow_nullable_key=1;

INSERT INTO 03164_users VALUES (1, 'John', 33);
INSERT INTO 03164_users VALUES (2, 'Ksenia', 48);
INSERT INTO 03164_users VALUES (NULL, 'Mark', 50);
OPTIMIZE TABLE 03164_users FINAL;

SELECT '-- Reproducer result:';

SELECT * FROM 03164_users ORDER BY uid ASC NULLS FIRST LIMIT 10 SETTINGS optimize_read_in_order = 1;

DROP TABLE IF EXISTS 03164_users;

DROP TABLE IF EXISTS 03164_multi_key;
CREATE TABLE 03164_multi_key (c1 Nullable(UInt32), c2 Nullable(UInt32)) ENGINE = MergeTree ORDER BY (c1, c2) SETTINGS allow_nullable_key=1;

INSERT INTO 03164_multi_key VALUES (0, 0), (1, NULL), (NULL, 2), (NULL, NULL), (4, 4);
-- Just in case
OPTIMIZE TABLE 03164_multi_key FINAL;

SELECT '';
SELECT '-- Read in order, no sort required:';

SELECT c1, c2
FROM 03164_multi_key
ORDER BY c1 ASC NULLS LAST, c2 ASC NULLS LAST
SETTINGS optimize_read_in_order = 1;

SELECT '';
SELECT '-- Read in order, partial sort for second key:';

SELECT c1, c2
FROM 03164_multi_key
ORDER BY c1 ASC NULLS LAST, c2 ASC NULLS FIRST
SETTINGS optimize_read_in_order = 1;

SELECT '';
SELECT '-- No reading in order, sort for first key:';

SELECT c1, c2
FROM 03164_multi_key
ORDER BY c1 ASC NULLS FIRST, c2 ASC NULLS LAST
SETTINGS optimize_read_in_order = 1;

SELECT '';
SELECT '-- Reverse order, partial sort for the second key:';

SELECT c1, c2
FROM 03164_multi_key
ORDER BY c1 DESC NULLS FIRST, c2 DESC NULLS LAST
SETTINGS optimize_read_in_order = 1;

DROP TABLE IF EXISTS 03164_multi_key;
