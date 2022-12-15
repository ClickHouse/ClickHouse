-- Tags: no-parallel

DROP TABLE IF EXISTS 02484_substitute_udf;
DROP FUNCTION IF EXISTS 02484_plusone;
DROP FUNCTION IF EXISTS 02484_plustwo;
DROP FUNCTION IF EXISTS 02484_plusthree;
DROP FUNCTION IF EXISTS 02484_plusthreemonths;
DROP FUNCTION IF EXISTS 02484_plusthreedays;

CREATE FUNCTION 02484_plusone AS (a) -> a + 1;
CREATE FUNCTION 02484_plustwo AS (a) -> a + 2;
CREATE FUNCTION 02484_plusthreemonths AS (a) -> a + INTERVAL 3 MONTH;

-- { echo }
CREATE TABLE 02484_substitute_udf (id UInt32, dt DateTime, number UInt32) 
ENGINE=MergeTree() 
ORDER BY 02484_plusone(id)
PARTITION BY 02484_plustwo(id)
SAMPLE BY 02484_plusone(id)
TTL 02484_plusthreemonths(dt);

SHOW CREATE TABLE 02484_substitute_udf;

CREATE FUNCTION 02484_plusthree AS (a) -> a + 3;
ALTER TABLE 02484_substitute_udf ADD COLUMN id2 UInt64, MODIFY ORDER BY (02484_plusone(id), 02484_plusthree(id2));
SHOW CREATE TABLE 02484_substitute_udf;

CREATE FUNCTION 02484_plusthreedays AS (a) -> a + INTERVAL 3 DAY;
ALTER TABLE 02484_substitute_udf MODIFY TTL 02484_plusthreedays(dt);
SHOW CREATE TABLE 02484_substitute_udf;

DROP TABLE 02484_substitute_udf;
DROP FUNCTION 02484_plusone;
DROP FUNCTION 02484_plustwo;
DROP FUNCTION 02484_plusthree;
DROP FUNCTION 02484_plusthreemonths;
DROP FUNCTION 02484_plusthreedays;
