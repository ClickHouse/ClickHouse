DROP TABLE IF EXISTS empty;
DROP TABLE IF EXISTS data;

SET allow_table_engine_tinylog=1;

CREATE TABLE empty (value Int8) ENGINE = TinyLog;
CREATE TABLE data (value Int8) ENGINE = TinyLog;

INSERT INTO data SELECT * FROM empty;
SELECT * FROM data;

INSERT INTO data SELECT 1;
SELECT * FROM data;

DROP TABLE empty;
DROP TABLE data;
