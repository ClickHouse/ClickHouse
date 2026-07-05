--https://github.com/ClickHouse/ClickHouse/issues/60232
DROP TABLE IF EXISTS hits1;
CREATE TABLE hits1
(
    `date` Date,
    `data` Array(UInt32)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY date;

INSERT INTO hits1 values('2024-01-01', [1, 2, 3]);

SELECT
    hits1.date,
    arrayFilter(x -> (x IN (2, 3)), data) AS filtered
FROM hits1
WHERE arrayExists(x -> (x IN (2, 3)), data)
SETTINGS enable_analyzer = 1;
