-- Tests that min-max statistics created over negative values work

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (
    i8 Int8,
    i16 Int16,
    i32 Int32,
    i64 Int64,
    i128 Int128,
    i256 Int256,
    bf16 BFloat16,
    f32 Float32,
    f64 Float32
)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS auto_statistics_types = 'minmax';

-- Insert a bunch of negative values
INSERT INTO tab VALUES (-100, -100, -100, -100, -100, -100, -100.0, -100.0, -100.0), (-50, -50, -50, -50, -50, -50, -50.0, -50.0, -50.0), (-1, -1, -1, -1, -1, -1, -1.0, -1.0, -1.0);

SELECT
    column,
    estimates.min,
    estimates.max
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'tab';

DROP TABLE tab;
