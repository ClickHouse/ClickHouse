DROP TABLE IF EXISTS file;
CREATE TABLE file (number UInt64) ENGINE = File(TSV);
SELECT * FROM file; -- { serverError 107 }
INSERT INTO file VALUES (1);
SELECT * FROM file;
DROP TABLE file;
