--https://github.com/ClickHouse/ClickHouse/issues/60232
CREATE TABLE hits
(
    `date` Date,
    `data` Array(UInt32)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY date;

INSERT INTO hits values('2024-01-01', [1, 2, 3]);

SELECT
    hits.date,
    arrayFilter(x -> (x IN (2, 3)), data) AS filtered
FROM hits
WHERE arrayExists(x -> (x IN (2, 3)), data)
SETTINGS enable_analyzer = 1;
