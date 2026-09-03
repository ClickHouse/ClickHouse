-- Tags: shard, no-fasttest

DROP TABLE IF EXISTS remote_test;
CREATE TABLE remote_test(a1 UInt8) ENGINE=Memory;
INSERT INTO FUNCTION remote(remote1, database=currentDatabase()) VALUES(1);
INSERT INTO FUNCTION remote(remote1, database=currentDatabase()) VALUES(2);
INSERT INTO FUNCTION remote(remote1, database=currentDatabase()) VALUES(3);
INSERT INTO FUNCTION remote(remote1, database=currentDatabase()) VALUES(4);
SELECT count() FROM remote(remote1, database=currentDatabase());
SELECT count() FROM remote(remote2, database=merge(currentDatabase(), '^remote_test'));
DROP TABLE remote_test;
