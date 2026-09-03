DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (c0 Int) ENGINE = Memory;
SELECT * FROM loop(remote('localhost:9000', currentDatabase(), 't0')) tx; -- { serverError TOO_MANY_RETRIES_TO_FETCH_PARTS }

INSERT INTO TABLE t0 SELECT * FROM  numbers(7);

SELECT '---';
SELECT * FROM loop(remote('localhost:9000', currentDatabase(), 't0')) tx LIMIT 3;

SELECT '---';
SELECT * FROM loop(remote('localhost:9000', currentDatabase(), 't0')) tx LIMIT 7;

SELECT '---';
SELECT * FROM loop(remote('localhost:9000', currentDatabase(), 't0')) tx LIMIT 11;

DROP TABLE t0 SYNC;
