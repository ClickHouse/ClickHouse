DROP TABLE IF EXISTS pk_func;
CREATE TABLE pk_func(d DateTime, ui UInt32) ENGINE = MergeTree ORDER BY toDate(d);

INSERT INTO pk_func SELECT '2020-05-05 01:00:00', number FROM numbers(1000000);
INSERT INTO pk_func SELECT '2020-05-06 01:00:00', number FROM numbers(1000000);
INSERT INTO pk_func SELECT '2020-05-07 01:00:00', number FROM numbers(1000000);

SELECT * FROM pk_func ORDER BY toDate(d), ui LIMIT 5;

DROP TABLE pk_func;

DROP TABLE IF EXISTS nORX;
CREATE TABLE nORX (`A` Int64, `B` Int64, `V` Int64) ENGINE = MergeTree ORDER BY (A, negate(B));
INSERT INTO nORX SELECT 111, number, number FROM numbers(10000000);

SELECT *
FROM nORX
WHERE B >= 1000
ORDER BY
    A ASC,
    -B ASC
LIMIT 3
SETTINGS max_threads = 1;

DROP TABLE nORX;
