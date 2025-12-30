SET enable_time_time64_type=1;

CREATE TEMPORARY TABLE test (
    t1 Time,
    t2 Time64(3),
    t3 DateTime64(3)
);

SET session_timezone='Europe/Amsterdam';

INSERT INTO test VALUES (36610, 36610.111::Decimal32(3), 33010.111::Decimal32(3));

SELECT t1, t2, t3 FROM test;

DROP TABLE test;
