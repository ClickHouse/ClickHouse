CREATE TEMPORARY TABLE test (
    t3 DateTime64(3)
);

SET session_timezone='Europe/Amsterdam';

INSERT INTO test VALUES (33010.111);

SELECT t3 FROM test;

DROP TABLE test;
