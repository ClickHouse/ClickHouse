-- Tags: no-parallel

DROP TABLE IF EXISTS _02483_substitute_udf;
DROP FUNCTION IF EXISTS _02483_plusone;
DROP FUNCTION IF EXISTS _02483_plustwo;
DROP FUNCTION IF EXISTS _02483_plusthree;

-- { echo }
CREATE FUNCTION _02483_plusone AS (a) -> a + 1;
CREATE TABLE _02483_substitute_udf (id UInt32, number UInt32 DEFAULT _02483_plusone(id)) ENGINE=MergeTree() ORDER BY id;
DESC TABLE _02483_substitute_udf;
INSERT INTO _02483_substitute_udf (id, number) VALUES (1, NULL);
SELECT * FROM _02483_substitute_udf ORDER BY id;

CREATE FUNCTION _02483_plustwo AS (a) -> a + 2;
ALTER TABLE _02483_substitute_udf MODIFY COLUMN number UInt32 DEFAULT _02483_plustwo(id);
DESC TABLE _02483_substitute_udf;
INSERT INTO _02483_substitute_udf (id, number) VALUES (5, NULL);
SELECT * FROM _02483_substitute_udf ORDER BY id;

CREATE FUNCTION _02483_plusthree AS (a) -> a + 3;
ALTER TABLE _02483_substitute_udf DROP COLUMN number;
ALTER TABLE _02483_substitute_udf ADD COLUMN new_number UInt32 DEFAULT _02483_plusthree(id);
DESC TABLE _02483_substitute_udf;
INSERT INTO _02483_substitute_udf (id, new_number) VALUES (10, NULL);
SELECT * FROM _02483_substitute_udf ORDER BY id;

DROP TABLE _02483_substitute_udf;
DROP FUNCTION _02483_plusone;
DROP FUNCTION _02483_plustwo;
DROP FUNCTION _02483_plusthree;
