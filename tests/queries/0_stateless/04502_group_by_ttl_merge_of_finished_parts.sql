-- Merging parts whose GROUP BY TTL is already finished must still re-aggregate
-- rows with the same key: the finished flag is a per-part property and is not
-- preserved when parts are combined.

DROP TABLE IF EXISTS t_ttl_finished;

CREATE TABLE t_ttl_finished (d DateTime, id UInt32, val UInt64)
ENGINE = MergeTree ORDER BY id
TTL d + INTERVAL 3 SECOND GROUP BY id SET val = sum(val);

-- The rows are already expired at insert time.
-- Roll up each part alone; this marks its GROUP BY TTL as finished.
INSERT INTO t_ttl_finished VALUES ('2020-01-01 00:00:00', 0, 1);
OPTIMIZE TABLE t_ttl_finished FINAL;

ALTER TABLE t_ttl_finished DETACH PARTITION ID 'all';

INSERT INTO t_ttl_finished VALUES ('2020-01-01 00:00:00', 0, 2);
OPTIMIZE TABLE t_ttl_finished FINAL;

ALTER TABLE t_ttl_finished ATTACH PARTITION ID 'all';

-- Combine the two finished parts.
OPTIMIZE TABLE t_ttl_finished FINAL;

SELECT val FROM t_ttl_finished ORDER BY val;

DROP TABLE t_ttl_finished;
