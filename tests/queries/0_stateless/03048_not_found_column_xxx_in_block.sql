-- https://github.com/ClickHouse/ClickHouse/issues/41964
SET enable_analyzer=1;

DROP TABLE IF EXISTS ab_12_aaa;
DROP TABLE IF EXISTS ab_12_bbb;

CREATE TABLE ab_12_aaa
(
    `id` String,
    `subid` Int32,
    `prodcat` String,
    `prodtype` String,
    `quality` String,
    `m1` Float64,
    `m2` Float64,
    `r1` Float64,
    `r2` Float64,
    `d1` Float64,
    `d2` Float64,
    `pcs` Float64,
    `qty` Float64,
    `amt` Float64,
    `amts` Float64,
    `prc` Float64,
    `prcs` Float64,
    `suqty` Float64,
    `suamt` Float64,
    `_year` String
)
ENGINE = MergeTree
ORDER BY (_year, prodcat, prodtype, quality, d1, id)
SETTINGS index_granularity = 8192;

CREATE TABLE ab_12_bbb
(
    `id` String,
    `sales_type` String,
    `date` Date32,
    `o1` String,
    `o2` String,
    `o3` String,
    `o4` String,
    `o5` String,
    `short` String,
    `a1` String,
    `a2` String,
    `a3` String,
    `idx` String,
    `a4` String,
    `ctx` String,
    `_year` String,
    `theyear` UInt16 MATERIALIZED toYear(`date`),
    `themonth` UInt8 MATERIALIZED toMonth(`date`),
    `theweek` UInt8 MATERIALIZED toISOWeek(`date`)
)
ENGINE = MergeTree
ORDER BY (theyear, themonth, _year, id, sales_type, date)
SETTINGS index_granularity = 8192;

SELECT *
FROM ab_12_aaa aa
LEFT JOIN ab_12_bbb bb
ON bb.id = aa.id AND bb.`_year` = aa.`_year`
WHERE bb.theyear >= 2019;

DROP TABLE IF EXISTS ab_12_aaa;
DROP TABLE IF EXISTS ab_12_bbb;
