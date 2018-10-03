DROP TABLE IF EXISTS test.src;
DROP TABLE IF EXISTS test.mv1;
DROP TABLE IF EXISTS test.mv2;

USE test;

CREATE TABLE src (x UInt8) ENGINE = Null;
CREATE MATERIALIZED VIEW mv1 ENGINE = Memory AS SELECT x FROM src WHERE x % 2 = 0;
CREATE MATERIALIZED VIEW mv2 ENGINE = Memory AS SELECT x FROM src WHERE x % 2 = 1;

SET parallel_view_processing = 1;
INSERT INTO src VALUES (1), (2);

SET parallel_view_processing = 0;
INSERT INTO src VALUES (3), (4);

SELECT * FROM mv1 ORDER BY x;
SELECT * FROM mv2 ORDER BY x;

DROP TABLE test.mv1;
DROP TABLE test.mv2;
DROP TABLE test.src;