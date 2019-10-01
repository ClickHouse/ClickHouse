CREATE DATABASE testlazy ENGINE = Lazy(30);
CREATE TABLE testlazy.log (a UInt64, b UInt64) ENGINE = Log;
CREATE TABLE testlazy.slog (a UInt64, b UInt64) ENGINE = StripeLog;
CREATE TABLE testlazy.tlog (a UInt64, b UInt64) ENGINE = TinyLog;

SELECT * FROM system.tables WHERE database = 'testlazy';
SELECT database, name, metadata_modification_time FROM system.tables WHERE database = 'testlazy';

INSERT INTO testlazy.log VALUES (1, 1);
INSERT INTO testlazy.slog VALUES (2, 2);
INSERT INTO testlazy.tlog VALUES (3, 3);
SELECT * FROM testlazy.log;
SELECT * FROM testlazy.slog;
SELECT * FROM testlazy.tlog;


DROP TABLE testlazy.log;

SELECT * FROM testlazy.slog;
SELECT * FROM testlazy.tlog;

DROP DATABASE testlazy;
