DROP TABLE IF EXISTS t;

CREATE TABLE t(n UInt32, s String) engine=Log AS SELECT * from generateRandom() limit 10;

select count() from t;

DROP TABLE t;
