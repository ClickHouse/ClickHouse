-- https://github.com/ClickHouse/ClickHouse/issues/91380
DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (s Tuple(a String)) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 0.1, serialization_info_version = 'with_types', string_serialization_version = 'single_stream';
INSERT INTO t0 values (('')), (('')), (('s'));
SELECT s.a, s.a.size FROM t0 ORDER BY ALL;

DROP TABLE t0;
