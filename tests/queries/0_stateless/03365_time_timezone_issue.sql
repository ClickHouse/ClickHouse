CREATE TEMPORARY TABLE test (
    t1 Time,
    t2 Time64(3),
    t3 DateTime64(3)
);

SET session_timezone='Europe/Amsterdam';

INSERT INTO test VALUES ('10:10:10', '10:10:10.111', '1970-01-01 10:10:10.111');

SELECT t1, t2, t3 FROM test;
