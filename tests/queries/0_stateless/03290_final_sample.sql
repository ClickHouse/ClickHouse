DROP TABLE IF EXISTS t_sample_final;

CREATE TABLE t_sample_final
(
    CounterID UInt32,
    UserID UInt64,
    Version UInt64
)
ENGINE = ReplacingMergeTree(Version)
ORDER BY (CounterID, intHash32(UserID))
SAMPLE BY intHash32(UserID)
SETTINGS index_granularity = 8192;

INSERT INTO t_sample_final SELECT * FROM generateRandom('CounterID UInt32, UserID UInt64, Version UInt64', 1, 10, 2) LIMIT 10;

OPTIMIZE TABLE t_sample_final FINAL;

set send_logs_level = 'error';

SET split_parts_ranges_into_intersecting_and_non_intersecting_final = 0, split_intersecting_parts_ranges_into_layers_final = 0;
SELECT count() FROM t_sample_final FINAL SAMPLE 1 / 2 WHERE NOT ignore(CounterID);

SET split_parts_ranges_into_intersecting_and_non_intersecting_final = 0, split_intersecting_parts_ranges_into_layers_final = 1;
SELECT count() FROM t_sample_final FINAL SAMPLE 1 / 2 WHERE NOT ignore(CounterID);

SET split_parts_ranges_into_intersecting_and_non_intersecting_final = 1, split_intersecting_parts_ranges_into_layers_final = 0;
SELECT count() FROM t_sample_final FINAL SAMPLE 1 / 2 WHERE NOT ignore(CounterID);

SET split_parts_ranges_into_intersecting_and_non_intersecting_final = 1, split_intersecting_parts_ranges_into_layers_final = 1;
SELECT count() FROM t_sample_final FINAL SAMPLE 1 / 2 WHERE NOT ignore(CounterID);

SET split_parts_ranges_into_intersecting_and_non_intersecting_final = 0, split_intersecting_parts_ranges_into_layers_final = 0;
SELECT count() FROM t_sample_final FINAL SAMPLE 1 / 2 WHERE NOT ignore(*);

SET split_parts_ranges_into_intersecting_and_non_intersecting_final = 0, split_intersecting_parts_ranges_into_layers_final = 1;
SELECT count() FROM t_sample_final FINAL SAMPLE 1 / 2 WHERE NOT ignore(*);

SET split_parts_ranges_into_intersecting_and_non_intersecting_final = 1, split_intersecting_parts_ranges_into_layers_final = 0;
SELECT count() FROM t_sample_final FINAL SAMPLE 1 / 2 WHERE NOT ignore(*);

SET split_parts_ranges_into_intersecting_and_non_intersecting_final = 1, split_intersecting_parts_ranges_into_layers_final = 1;
SELECT count() FROM t_sample_final FINAL SAMPLE 1 / 2 WHERE NOT ignore(*);

DROP TABLE t_sample_final;
