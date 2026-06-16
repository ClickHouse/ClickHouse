-- Tags: memory-engine
DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (c0 Nullable(Int)) ENGINE = Memory() SETTINGS compress = 1;
INSERT INTO TABLE t0 (c0) VALUES (1);

SELECT t0.c0.null FROM t0 FORMAT Null SETTINGS enable_analyzer = 1;

DROP TABLE t0;
