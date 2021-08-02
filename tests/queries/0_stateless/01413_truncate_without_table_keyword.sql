DROP TABLE IF EXISTS truncate_test;

CREATE TABLE truncate_test(uint8 UInt8) ENGINE = Log;

INSERT INTO truncate_test VALUES(1), (2), (3);

SELECT * FROM truncate_test ORDER BY uint8;

TRUNCATE truncate_test;

SELECT * FROM truncate_test ORDER BY uint8;

