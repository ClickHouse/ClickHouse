DROP TABLE IF EXISTS t;
CREATE TABLE t (x UInt8) ENGINE = TinyLog;

SET max_execution_time = 1, lock_acquire_timeout = 1000;
INSERT INTO t SELECT * FROM t; -- { serverError 159 }

SET max_execution_time = 0, lock_acquire_timeout = 1;
INSERT INTO t SELECT * FROM t; -- { serverError 159 }

DROP TABLE t;


SET max_execution_time = 0, lock_acquire_timeout = 1000;

CREATE TABLE t (x UInt8) ENGINE = Log;

INSERT INTO t VALUES (1), (2), (3);
INSERT INTO t SELECT * FROM t;
SELECT count() FROM t;

DROP TABLE t;


CREATE TABLE t (x UInt8) ENGINE = StripeLog;

INSERT INTO t VALUES (1), (2), (3);
INSERT INTO t SELECT * FROM t;
SELECT count() FROM t;

DROP TABLE t;
