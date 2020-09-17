DROP TABLE IF EXISTS t;
CREATE TABLE t (x UInt8) ENGINE = TinyLog;

SET max_execution_time = 1;
INSERT INTO t SELECT * FROM t; -- { serverError 159 }

SET max_execution_time = 0, lock_acquire_timeout = 1;
INSERT INTO t SELECT * FROM t; -- { serverError 159 }

DROP TABLE t;
