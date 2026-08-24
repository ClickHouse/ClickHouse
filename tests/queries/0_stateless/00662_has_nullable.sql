DROP TABLE IF EXISTS 00662_has_nullable;

SELECT 'Nullable(UInt64), non-null array';
CREATE TABLE 00662_has_nullable(a Nullable(UInt64)) ENGINE = Memory;

INSERT INTO 00662_has_nullable VALUES (1), (Null);
SELECT a, has([0, 1], a) FROM 00662_has_nullable;

DROP TABLE 00662_has_nullable;

--------------------------------------------------------------------------------

SELECT 'Non-nullable UInt64, nullable array';
CREATE TABLE 00662_has_nullable(a UInt64) ENGINE = Memory;

INSERT INTO 00662_has_nullable VALUES (0), (1), (2);
SELECT a, has([NULL, 1, 2], a) FROM 00662_has_nullable;

DROP TABLE 00662_has_nullable;

--------------------------------------------------------------------------------

SELECT 'Nullable(UInt64), nullable array';
CREATE TABLE 00662_has_nullable(a Nullable(UInt64)) ENGINE = Memory;

INSERT INTO 00662_has_nullable VALUES (0), (Null), (1);
SELECT a, has([NULL, 1, 2], a) FROM 00662_has_nullable;

DROP TABLE 00662_has_nullable;

--------------------------------------------------------------------------------

SELECT 'All NULLs';
CREATE TABLE 00662_has_nullable(a Nullable(UInt64)) ENGINE = Memory;

INSERT INTO 00662_has_nullable VALUES (0), (Null);
SELECT a, has([NULL, NULL], a) FROM 00662_has_nullable;

DROP TABLE 00662_has_nullable;
