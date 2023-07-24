DROP TABLE IF EXISTS t_projections_lwd;

CREATE TABLE t_projections_lwd (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_projections_lwd SELECT number, number FROM numbers(100);

-- LWD works
DELETE FROM t_projections_lwd WHERE a = 0;

-- add projection
ALTER TABLE t_projections_lwd ADD PROJECTION p_t_projections_lwd (SELECT * ORDER BY b);
ALTER TABLE t_projections_lwd MATERIALIZE PROJECTION p_t_projections_lwd;

-- LWD does not work, as expected
DELETE FROM t_projections_lwd WHERE a = 1; -- { serverError UNFINISHED }
KILL MUTATION WHERE database = currentDatabase() AND table = 't_projections_lwd' SYNC FORMAT Null;

-- drop projection
SET mutations_sync = 2;
ALTER TABLE t_projections_lwd DROP projection p_t_projections_lwd;

DELETE FROM t_projections_lwd WHERE a = 2;

SELECT count() FROM t_projections_lwd;

DROP TABLE t_projections_lwd;
