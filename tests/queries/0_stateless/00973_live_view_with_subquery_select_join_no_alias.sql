SET allow_experimental_live_view = 1;

DROP TABLE IF EXISTS lv;
DROP TABLE IF EXISTS A;
DROP TABLE IF EXISTS B;

CREATE TABLE A (id Int32) Engine=Memory;
CREATE TABLE B (id Int32, name String) Engine=Memory;

CREATE LIVE VIEW lv AS SELECT id, name FROM ( SELECT A.id, B.name FROM A, B WHERE A.id = B.id);

SELECT * FROM lv;

INSERT INTO A VALUES (1);
INSERT INTO B VALUES (1, 'hello');

SELECT *,_version FROM lv ORDER BY id;
SELECT *,_version FROM lv ORDER BY id;

INSERT INTO A VALUES (2)
INSERT INTO B VALUES (2, 'hello')

SELECT *,_version FROM lv ORDER BY id;
SELECT *,_version FROM lv ORDER BY id;

DROP TABLE lv;
DROP TABLE A;
DROP TABLE B;
