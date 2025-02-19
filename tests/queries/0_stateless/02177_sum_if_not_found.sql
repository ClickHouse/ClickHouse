SELECT sumIf(1, 0);
SELECT SumIf(1, 0);
SELECT sUmIf(1, 0);
SELECT sumIF(1, 0); -- { serverError 46 }

DROP TABLE IF EXISTS data;
DROP TABLE IF EXISTS agg;

CREATE TABLE data
(
    `n` UInt32,
    `t` DateTime
)
ENGINE = Null;

CREATE TABLE agg
ENGINE = AggregatingMergeTree
ORDER BY tuple() AS
SELECT
    t,
    sumIF(n, 0)
FROM data
GROUP BY t; -- { serverError 46}

CREATE TABLE agg
ENGINE = AggregatingMergeTree
ORDER BY tuple() AS
SELECT
    t,
    sumIf(n, 0)
FROM data
GROUP BY t;

DROP TABLE data;
DROP TABLE agg;
