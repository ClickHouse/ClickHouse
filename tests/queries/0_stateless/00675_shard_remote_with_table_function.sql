-- Tags: shard

DROP TABLE IF EXISTS remote_test;
CREATE TABLE remote_test(a1 UInt8) ENGINE=Memory;
INSERT INTO FUNCTION remote('127.0.0.1', currentDatabase(), remote_test) VALUES(1);
INSERT INTO FUNCTION remote('127.0.0.1', currentDatabase(), remote_test) VALUES(2);
INSERT INTO FUNCTION remote('127.0.0.1', currentDatabase(), remote_test) VALUES(3);
INSERT INTO FUNCTION remote('127.0.0.1', currentDatabase(), remote_test) VALUES(4);
SELECT COUNT(*) FROM remote('127.0.0.1', currentDatabase(), remote_test);
SELECT count(*) FROM remote('127.0.0.{1,2}', merge(currentDatabase(), '^remote_test'));
DROP TABLE remote_test;
