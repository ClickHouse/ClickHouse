DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Date) ENGINE = MergeTree() ORDER BY () TTL (materialize(c0));
DROP TABLE t0;
