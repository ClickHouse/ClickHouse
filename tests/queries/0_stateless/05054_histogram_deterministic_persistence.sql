-- Tags: no-fasttest
-- no-fasttest: histogram statistics use the DataSketches KLL library

SET allow_statistics = 1;
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS histogram_determinism_1;
DROP TABLE IF EXISTS histogram_determinism_2;

CREATE TABLE histogram_determinism_1
(
    id UInt64,
    value Float64 STATISTICS(histogram(128))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS auto_statistics_types = '';

CREATE TABLE histogram_determinism_2
(
    id UInt64,
    value Float64 STATISTICS(histogram(128))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS auto_statistics_types = '';

INSERT INTO histogram_determinism_1 SELECT number, number * number FROM numbers(10000);
INSERT INTO histogram_determinism_1 SELECT number + 10000, number * 3 FROM numbers(10000);
INSERT INTO histogram_determinism_2 SELECT number, number * number FROM numbers(10000);
INSERT INTO histogram_determinism_2 SELECT number + 10000, number * 3 FROM numbers(10000);

OPTIMIZE TABLE histogram_determinism_1 FINAL;
OPTIMIZE TABLE histogram_determinism_2 FINAL;

SELECT first.hash_of_all_files = second.hash_of_all_files
FROM
(
    SELECT hash_of_all_files
    FROM system.parts
    WHERE database = currentDatabase() AND table = 'histogram_determinism_1' AND active
) AS first
CROSS JOIN
(
    SELECT hash_of_all_files
    FROM system.parts
    WHERE database = currentDatabase() AND table = 'histogram_determinism_2' AND active
) AS second;

DROP TABLE histogram_determinism_1;
DROP TABLE histogram_determinism_2;
