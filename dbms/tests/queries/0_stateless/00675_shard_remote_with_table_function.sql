DROP TABLE IF EXISTS test.remote_test;
CREATE TABLE test.remote_test(a1 UInt8) ENGINE=Memory;
INSERT INTO FUNCTION remote('127.0.0.1', test.remote_test) VALUES(1);
INSERT INTO FUNCTION remote('127.0.0.1', test.remote_test) VALUES(2);
INSERT INTO FUNCTION remote('127.0.0.1', test.remote_test) VALUES(3);
INSERT INTO FUNCTION remote('127.0.0.1', test.remote_test) VALUES(4);
SELECT COUNT(*) FROM remote('127.0.0.1', test.remote_test);
SELECT count(*) FROM remote('127.0.0.{1,2}', merge(test, '^remote_test'));
DROP TABLE test.remote_test;
