-- https://github.com/ClickHouse/ClickHouse/issues/90240
-- toWeek() incorrectly claimed monotonicity, causing partition pruning
-- to skip December partitions for weeks 49-52.

DROP TABLE IF EXISTS test_toweek_pruning;

CREATE TABLE test_toweek_pruning (date Date, value String)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY date;

INSERT INTO test_toweek_pruning VALUES
    ('2025-11-30', 'x'), ('2025-12-01', 'x'), ('2025-12-07', 'x'),
    ('2025-12-08', 'x'), ('2025-12-14', 'x'), ('2025-12-15', 'x'),
    ('2025-12-21', 'x'), ('2025-12-22', 'x'), ('2025-12-28', 'x'),
    ('2025-12-29', 'x'), ('2025-12-31', 'x');

SELECT toWeek(date, 3), count() FROM test_toweek_pruning WHERE toWeek(date, 3) = 49 GROUP BY 1;
SELECT toWeek(date, 3), count() FROM test_toweek_pruning WHERE toWeek(date, 3) = 52 GROUP BY 1;

DROP TABLE test_toweek_pruning;
