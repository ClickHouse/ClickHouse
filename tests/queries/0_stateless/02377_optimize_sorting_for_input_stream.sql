DROP TABLE IF EXISTS optimize_sorting;
CREATE TABLE optimize_sorting (a UInt64, b UInt64, c UInt64) ENGINE MergeTree() ORDER BY (a, b);
INSERT INTO optimize_sorting SELECT number, number%5, number%2 from numbers(10);
-- { echoOn }
SELECT a as a from optimize_sorting order by a;
SELECT sipHash64(a) as a from optimize_sorting order by a;
-- { echoOff }

DROP TABLE IF EXISTS optimize_sorting;

