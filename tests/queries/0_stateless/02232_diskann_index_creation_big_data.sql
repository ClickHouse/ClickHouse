DROP TABLE IF EXISTS t_diskann_test;

CREATE TABLE t_diskann_test
(
  `id` Int64,
  `number` Tuple(Float32, Float32, Float32),
  INDEX x number TYPE diskann GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_diskann_test SELECT
    number AS id,
    (toFloat32(number), toFloat32(number), toFloat32(number))
FROM system.numbers
LIMIT 100000; -- to create more than 1 granules

SELECT * from t_diskann_test LIMIT 1000;
