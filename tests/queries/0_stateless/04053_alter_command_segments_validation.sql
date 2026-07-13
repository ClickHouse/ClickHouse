-- Tags: no-replicated-database
-- Tag no-replicated-database: Test uses combination of different alter segments

DROP TABLE IF EXISTS t_alter_segments;

CREATE TABLE t_alter_segments (i Int32) ENGINE = MergeTree ORDER BY i;
INSERT INTO t_alter_segments VALUES (1), (2), (3);

ALTER TABLE t_alter_segments ADD COLUMN s String DEFAULT toString(i*i);
ALTER TABLE t_alter_segments
    MATERIALIZE COLUMN s,  -- segment 1: mutation
    RENAME COLUMN s TO s2  -- segment 2: alter
    SETTINGS mutations_sync = 2, alter_sync = 2;

SELECT i, s2 FROM t_alter_segments ORDER BY i;

DROP TABLE IF EXISTS t_alter_segments;
