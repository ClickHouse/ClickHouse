DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (s Nullable(String)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 10, ratio_of_defaults_for_sparse_serialization = 0.001, nullable_serialization_version = 'allow_sparse', min_bytes_for_wide_part='100G';
INSERT INTO t0 SELECT if((number % 2) = 0, toString(number), NULL) FROM numbers(30);
SELECT s.null, s.size from t0;
DROP TABLE t0;

