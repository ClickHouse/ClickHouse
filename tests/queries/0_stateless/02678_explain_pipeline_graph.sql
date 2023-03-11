-- The server does not crash after these queries:

DROP TABLE IF EXISTS t1;
CREATE TABLE t1(ID UInt64, name String) engine=MergeTree order by ID;
insert into t1(ID, name) values (1, 'abc'), (2, 'bbb');
explain pipeline graph=1 select count(ID) from t1 FORMAT Null;
DROP TABLE t1;
