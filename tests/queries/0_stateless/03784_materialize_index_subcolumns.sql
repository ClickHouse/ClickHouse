DROP TABLE IF EXISTS t_index;

CREATE TABLE t_index (data JSON(a UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_index (data) VALUES ('{"a": 1, "b": 11}');
SET mutations_sync = 2;

ALTER TABLE t_index ADD INDEX a_idx(data.a) TYPE minmax;
ALTER TABLE t_index MATERIALIZE INDEX a_idx;

ALTER TABLE t_index ADD INDEX b_idx(data.b::UInt64) TYPE minmax;
ALTER TABLE t_index MATERIALIZE INDEX b_idx;

SELECT sum(secondary_indices_compressed_bytes) > 0 FROM system.parts WHERE database = currentDatabase() AND table = 't_index' AND active;

SELECT count() FROM t_index WHERE data.a = 1 SETTINGS force_data_skipping_indices = 'a_idx';
SELECT count() FROM t_index WHERE data.b::UInt64 = 11 SETTINGS force_data_skipping_indices = 'b_idx';

DROP TABLE IF EXISTS t_index;

CREATE TABLE t_index (id UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 100000000;

INSERT INTO t_index (id) VALUES (1);

ALTER TABLE t_index ADD COLUMN data JSON(a UInt64);

SELECT column FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_index' AND active;

ALTER TABLE t_index ADD INDEX a_idx(data.a) TYPE minmax;
ALTER TABLE t_index MATERIALIZE INDEX a_idx;

ALTER TABLE t_index ADD INDEX b_idx(data.b::UInt64) TYPE minmax;
ALTER TABLE t_index MATERIALIZE INDEX b_idx;

-- Check that column 'data' was materialized on MATERIALIZE INDEX query.
SELECT column FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_index' AND active ORDER BY column;
SELECT sum(secondary_indices_compressed_bytes) > 0 FROM system.parts WHERE database = currentDatabase() AND table = 't_index' AND active;

SELECT count() FROM t_index WHERE data.a = 1 SETTINGS force_data_skipping_indices = 'a_idx';
SELECT count() FROM t_index WHERE data.b::UInt64 = 11 SETTINGS force_data_skipping_indices = 'b_idx';

DROP TABLE IF EXISTS t_index;
