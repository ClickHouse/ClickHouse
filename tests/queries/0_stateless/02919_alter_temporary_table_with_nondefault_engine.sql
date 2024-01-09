DROP TABLE IF EXISTS alter_test;

CREATE TEMPORARY TABLE alter_test (a UInt32, b UInt8) ENGINE=MergeTree ORDER BY a;
INSERT INTO alter_test VALUES (1, 2);
ALTER TEMPORARY TABLE alter_test MODIFY COLUMN b UInt8 FIRST;
DESC TABLE alter_test;

DROP TABLE IF EXISTS alter_test;

CREATE TEMPORARY TABLE alter_test (a UInt32, b UInt8) ENGINE=Log;
INSERT INTO alter_test VALUES (1, 2);
ALTER TEMPORARY TABLE alter_test COMMENT COLUMN b 'this is comment for log engine';
DESC TABLE alter_test;

DROP TABLE IF EXISTS alter_test;

CREATE TEMPORARY TABLE alter_test (a UInt32, b UInt8) ENGINE=Null;
INSERT INTO alter_test VALUES (1, 2);
ALTER TEMPORARY TABLE alter_test MODIFY COLUMN b UInt8 FIRST;
DESC TABLE alter_test;

