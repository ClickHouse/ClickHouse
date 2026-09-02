-- https://github.com/ClickHouse/ClickHouse/issues/91380
DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (s Nullable(String), t Tuple(a Nullable(String))) ENGINE = MergeTree ORDER BY tuple() SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, serialization_info_version = 'with_types', nullable_serialization_version = 'allow_sparse';
INSERT INTO t0 SELECT if((number % 13) = 0, toString(number), NULL), (if((number % 11) = 0, number, NULL),) FROM numbers(1000);
SELECT * FROM t0 FULL JOIN t0 ty ON ty.s = t.a WHERE t.a.size = 1 ORDER BY ALL;

DROP TABLE t0;

CREATE TABLE t0 (s Tuple(a String)) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 0.1, serialization_info_version = 'with_types', string_serialization_version = 'single_stream';
INSERT INTO t0 values (('')), (('')), (('s'));
SELECT s.a, s.a.size FROM t0 ORDER BY ALL;

DROP TABLE t0;
