DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Dynamic) ENGINE = MergeTree() ORDER BY tuple() SETTINGS index_granularity_bytes = 0, adaptive_write_buffer_initial_size = 0; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t0 (c0 Dynamic) ENGINE = MergeTree() ORDER BY tuple() SETTINGS index_granularity_bytes = 0, adaptive_write_buffer_initial_size = 1;
INSERT INTO TABLE t0 (c0) VALUES (1);
SELECT c0 FROM t0;
DROP TABLE t0;
