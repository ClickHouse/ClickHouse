DROP TABLE IF EXISTS data2013;
DROP TABLE IF EXISTS data2015;

CREATE TABLE data2013 (name String, value UInt32) ENGINE = Memory;
CREATE TABLE data2015 (data_name String, data_value UInt32) ENGINE = Memory;

INSERT INTO data2013(name,value) VALUES('Alice', 1000);
INSERT INTO data2013(name,value) VALUES('Bob', 2000);
INSERT INTO data2013(name,value) VALUES('Carol', 5000);

INSERT INTO data2015(data_name, data_value) VALUES('Foo', 42);
INSERT INTO data2015(data_name, data_value) VALUES('Bar', 1);

SELECT name FROM (SELECT name FROM data2013 UNION ALL SELECT data_name FROM data2015) ORDER BY name ASC;

DROP TABLE data2013;
DROP TABLE data2015;
