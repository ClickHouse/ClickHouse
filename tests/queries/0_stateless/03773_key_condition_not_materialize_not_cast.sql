-- Tags: no-replicated-database, no-parallel-replicas, no-parallel, no-random-merge-tree-settings
-- EXPLAIN output may differ

DROP TABLE IF EXISTS t_cast_bug;

CREATE TABLE t_cast_bug (val UInt8) ENGINE = MergeTree ORDER BY val;
SYSTEM STOP MERGES t_cast_bug;

INSERT INTO t_cast_bug VALUES (1);
INSERT INTO t_cast_bug VALUES (0);
INSERT INTO t_cast_bug VALUES (0), (2);

SELECT val FROM t_cast_bug WHERE NOT CAST(val = 0, 'UInt8') ORDER BY val;

EXPLAIN indexes=1
SELECT val FROM t_cast_bug WHERE NOT CAST(val = 0, 'UInt8') ORDER BY val;


DROP TABLE IF EXISTS t_materialize_bug;

CREATE TABLE t_materialize_bug (val UInt8) ORDER BY (val);
CREATE VIEW v AS SELECT val, val = 0 AS is_zero FROM t_materialize_bug;
SYSTEM STOP MERGES t_materialize_bug;

INSERT INTO t_materialize_bug VALUES (1);
INSERT INTO t_materialize_bug VALUES (0);
INSERT INTO t_materialize_bug VALUES (0), (2);

SELECT val FROM v WHERE NOT is_zero ORDER BY val;

EXPLAIN indexes=1
SELECT val FROM v WHERE NOT is_zero ORDER BY val;

