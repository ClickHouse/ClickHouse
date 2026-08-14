-- Tags: no-fasttest

-- A projection's storage type (packed vs full) always follows its parent part, because the projection
-- builder is created from the parent's storage. The part-storage-selection settings must therefore be
-- rejected for projections rather than silently ignored. Guards both CREATE and ALTER ADD PROJECTION.

DROP TABLE IF EXISTS t_proj_reject SYNC;

CREATE TABLE t_proj_reject (id UInt64, v UInt64, PROJECTION p (SELECT v, count() GROUP BY v) WITH SETTINGS (min_bytes_for_full_part_storage = 0))
ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_proj_reject (id UInt64, v UInt64, PROJECTION p (SELECT v, count() GROUP BY v) WITH SETTINGS (min_rows_for_full_part_storage = 0))
ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_proj_reject (id UInt64, v UInt64, PROJECTION p (SELECT v, count() GROUP BY v) WITH SETTINGS (min_level_for_full_part_storage = 0))
ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_proj_reject (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;

ALTER TABLE t_proj_reject ADD PROJECTION p (SELECT v, count() GROUP BY v) WITH SETTINGS (min_bytes_for_full_part_storage = 0); -- { serverError BAD_ARGUMENTS }

DROP TABLE t_proj_reject SYNC;
