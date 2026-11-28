-- A part with a pending `DROP COLUMN` / `RENAME COLUMN` mutation is read from the parent part instead of
-- its projection part only when the mutation touches a column the projection holds or the query reads.
-- A pending drop or rename of an unrelated column keeps the projection usable.

DROP TABLE IF EXISTS t_proj_unrelated_drop;
CREATE TABLE t_proj_unrelated_drop (id UInt64, x UInt64, y UInt64, PROJECTION p (SELECT id, x ORDER BY x))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 128;
INSERT INTO t_proj_unrelated_drop SELECT number, number % 100, number FROM numbers(100000);

SYSTEM STOP MERGES t_proj_unrelated_drop;
ALTER TABLE t_proj_unrelated_drop DROP COLUMN y SETTINGS alter_sync = 0;

SELECT count() FROM t_proj_unrelated_drop WHERE x = 5 SETTINGS force_optimize_projection = 1;
SELECT count() FROM t_proj_unrelated_drop WHERE x = 5 SETTINGS optimize_use_projections = 0;

DROP TABLE IF EXISTS t_proj_unrelated_rename;
CREATE TABLE t_proj_unrelated_rename (id UInt64, x UInt64, z UInt64, PROJECTION p (SELECT id, x ORDER BY x))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 128;
INSERT INTO t_proj_unrelated_rename SELECT number, number % 100, number FROM numbers(100000);

SYSTEM STOP MERGES t_proj_unrelated_rename;
ALTER TABLE t_proj_unrelated_rename RENAME COLUMN z TO w SETTINGS alter_sync = 0;

SELECT count() FROM t_proj_unrelated_rename WHERE x = 5 SETTINGS force_optimize_projection = 1;
SELECT count(), sum(w) FROM t_proj_unrelated_rename WHERE x = 5 SETTINGS optimize_use_projections = 0;

DROP TABLE t_proj_unrelated_drop;
DROP TABLE t_proj_unrelated_rename;
