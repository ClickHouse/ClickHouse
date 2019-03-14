USE test;
DROP TABLE IF EXISTS t_local;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t_local (dummy UInt8) ENGINE = Memory;
CREATE TABLE t1 (dummy UInt8) ENGINE = Distributed(test_shard_localhost, 'test', 't_local');
CREATE TABLE t2 (dummy UInt8) ENGINE = Distributed(test_shard_localhost, 'test', 't_local');

INSERT INTO t_local VALUES (1);

SET asterisk_left_columns_only = 1;

SELECT * FROM t1
GLOBAL INNER JOIN
(
    SELECT *
    FROM ( SELECT * FROM t2 )
    INNER JOIN ( SELECT * FROM t1 )
    USING dummy
) USING dummy;

DROP TABLE t_local;
DROP TABLE t1;
DROP TABLE t2;


SELECT * FROM remote('127.0.0.2', system.one)
GLOBAL INNER JOIN
(
    SELECT *
    FROM ( SELECT dummy FROM remote('127.0.0.2', system.one) ) t1
    GLOBAL INNER JOIN ( SELECT dummy FROM remote('127.0.0.3', system.one) ) t2
    USING dummy
) USING dummy;
