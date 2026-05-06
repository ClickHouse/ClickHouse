SET allow_suspicious_low_cardinality_types=1;

SELECT 'float 32';
CREATE TABLE table_f32 (c0 LowCardinality(Float32), c1 Int) ENGINE = MergeTree() ORDER BY (c0, c1);
INSERT INTO table_f32 VALUES (nan, 3), (-nan, 2), (nan, 1);
SELECT * FROM table_f32 ORDER BY all;

SELECT 'float 64';
CREATE TABLE table_f64 (c0 LowCardinality(Float64), c1 Int) ENGINE = MergeTree() ORDER BY (c0, c1);
INSERT INTO table_f64 VALUES (nan, 3), (-nan, 2), (nan, 1);
SELECT * FROM table_f64 ORDER BY all;

SELECT 'bfloat 16';
CREATE TABLE table_f16 (c0 LowCardinality(BFloat16), c1 Int) ENGINE = MergeTree() ORDER BY (c0, c1);
INSERT INTO table_f16 VALUES (nan, 3), (-nan, 2), (nan, 1);
SELECT * FROM table_f16 ORDER BY all;
