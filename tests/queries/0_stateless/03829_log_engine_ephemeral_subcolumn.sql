DROP TABLE IF EXISTS test;
CREATE TABLE test (c0 Tuple(c1 Nullable(DateTime64(6))) EPHEMERAL, c2 Nullable(Bool)) ENGINE = TinyLog;
INSERT INTO test (c2) VALUES (true);
SELECT c0.c1.null FROM test; -- { serverError UNKNOWN_IDENTIFIER }
DROP TABLE test;
