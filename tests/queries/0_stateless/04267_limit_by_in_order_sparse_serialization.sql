DROP TABLE IF EXISTS t;

CREATE TABLE t (k UInt32, v UInt32)
ENGINE = MergeTree ORDER BY (k, v)
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.01;

INSERT INTO t SELECT 0, 0 FROM numbers(10);

SELECT k FROM t ORDER BY k LIMIT 1 BY k;

DROP TABLE t;
