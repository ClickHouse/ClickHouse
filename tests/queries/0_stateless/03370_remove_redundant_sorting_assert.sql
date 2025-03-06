DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Int) ENGINE = Memory;
INSERT INTO TABLE t0 (c0) VALUES (1);
SELECT 1 FROM (SELECT materialize(1) FROM remote('localhost:9000', currentDatabase(), t0) ORDER BY 1) ORDER BY 1;

DROP TABLE t0;
