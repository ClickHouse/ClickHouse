-- Tags: no-parallel

DROP TABLE IF EXISTS _02484_substitute_udf;
DROP FUNCTION IF EXISTS _02484_plusone;
DROP FUNCTION IF EXISTS _02484_plustwo;
DROP FUNCTION IF EXISTS _02484_plusthree;
DROP FUNCTION IF EXISTS _02484_plusthreemonths;
DROP FUNCTION IF EXISTS _02484_plusthreedays;

CREATE FUNCTION _02484_plusone AS (a) -> a + 1;
CREATE FUNCTION _02484_plustwo AS (a) -> a + 2;
CREATE FUNCTION _02484_plusthreemonths AS (a) -> a + INTERVAL 3 MONTH;

-- { echo }
CREATE TABLE _02484_substitute_udf (id UInt32, dt DateTime, number UInt32) 
ENGINE=MergeTree() 
ORDER BY _02484_plusone(id)
PARTITION BY _02484_plustwo(id)
SAMPLE BY _02484_plusone(id)
TTL _02484_plusthreemonths(dt);

SHOW CREATE TABLE _02484_substitute_udf;

CREATE FUNCTION _02484_plusthree AS (a) -> a + 3;
ALTER TABLE _02484_substitute_udf ADD COLUMN id2 UInt64, MODIFY ORDER BY (_02484_plusone(id), _02484_plusthree(id2));
SHOW CREATE TABLE _02484_substitute_udf;

CREATE FUNCTION _02484_plusthreedays AS (a) -> a + INTERVAL 3 DAY;
ALTER TABLE _02484_substitute_udf MODIFY TTL _02484_plusthreedays(dt);
SHOW CREATE TABLE _02484_substitute_udf;

DROP TABLE _02484_substitute_udf;
DROP FUNCTION _02484_plusone;
DROP FUNCTION _02484_plustwo;
DROP FUNCTION _02484_plusthree;
DROP FUNCTION _02484_plusthreemonths;
DROP FUNCTION _02484_plusthreedays;
