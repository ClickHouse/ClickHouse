set enable_analyzer=1;

DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (c0 Int, c1 Int ALIAS 1) ENGINE = Memory;
INSERT INTO t0 VALUES (42), (43);

SELECT c0 FROM remote('localhost', currentDatabase(), 't0') tx JOIN t0 USING (c1) ORDER BY c0;

CREATE TABLE t1_dist ( c0 Int, c1 Int, c2 Int ALIAS 2 ) ENGINE = Distributed('test_shard_localhost', currentDatabase(), 't0', rand());

SELECT c0 FROM t1_dist tx JOIN t0 USING (c1) ORDER BY c0;

-- Cannot join using alias column defined in Distributed table
SELECT c0 FROM t1_dist tx JOIN t0 USING (c2); -- { serverError UNKNOWN_IDENTIFIER }

DROP TABLE IF EXISTS t0;
