drop table if exists test;

set allow_suspicious_low_cardinality_types = 1;

CREATE TABLE test
(
    `timestamp` DateTime,
    `latitude` Nullable(Float32) CODEC(Gorilla, ZSTD(1)),
    `longitude` Nullable(Float32) CODEC(Gorilla, ZSTD(1)),
    `m_registered` UInt8,
    `m_mcc` Nullable(Int16),
    `m_mnc` Nullable(Int16),
    `m_ci` Nullable(Int32),
    `m_tac` Nullable(Int32),
    `enb_id` Nullable(Int32),
    `ci` Nullable(Int32),
    `m_earfcn` Int32,
    `rsrp` Nullable(Int16),
    `rsrq` Nullable(Int16),
    `cqi` Nullable(Int16),
    `source` String,
    `gps_accuracy` Nullable(Float32),
    `operator_name` String,
    `band` Nullable(String),
    `NAME_2` String,
    `NAME_1` String,
    `quadkey_19_key` FixedString(19),
    `quadkey_17_key` FixedString(17),
    `manipulation` UInt8,
    `ss_rsrp` Nullable(Int16),
    `ss_rsrq` Nullable(Int16),
    `ss_sinr` Nullable(Int16),
    `csi_rsrp` Nullable(Int16),
    `csi_rsrq` Nullable(Int16),
    `csi_sinr` Nullable(Int16),
    `altitude` Nullable(Float32),
    `access_technology` Nullable(String),
    `buildingtype` String,
    `LocationType` String,
    `carrier_name` Nullable(String),
    `CustomPolygonName` String,
    `h3_10_pixel` UInt64,
    `stc_cluster` Nullable(String),
    PROJECTION cumsum_projection_simple
    (
        SELECT
            m_registered,
            toStartOfInterval(timestamp, toIntervalMonth(1)),
            toStartOfWeek(timestamp, 8),
            toStartOfInterval(timestamp, toIntervalDay(1)),
            NAME_1,
            NAME_2,
            operator_name,
            rsrp,
            rsrq,
            ss_rsrp,
            ss_rsrq,
            cqi,
            sum(multiIf(ss_rsrp IS NULL, 0, 1)),
            sum(multiIf(ss_rsrq IS NULL, 0, 1)),
            sum(multiIf(ss_sinr IS NULL, 0, 1)),
            max(toStartOfInterval(timestamp, toIntervalDay(1))),
            max(CAST(CAST(toStartOfInterval(timestamp, toIntervalDay(1)), 'Nullable(DATE)'), 'Nullable(TIMESTAMP)')),
            min(toStartOfInterval(timestamp, toIntervalDay(1))),
            min(CAST(CAST(toStartOfInterval(timestamp, toIntervalDay(1)), 'Nullable(DATE)'), 'Nullable(TIMESTAMP)')),
            count(),
            sum(1)
        GROUP BY
            m_registered,
            toStartOfInterval(timestamp, toIntervalMonth(1)),
            toStartOfWeek(timestamp, 8),
            toStartOfInterval(timestamp, toIntervalDay(1)),
            m_registered,
            toStartOfInterval(timestamp, toIntervalMonth(1)),
            toStartOfWeek(timestamp, 8),
            toStartOfInterval(timestamp, toIntervalDay(1)),
            NAME_1,
            NAME_2,
            operator_name,
            rsrp,
            rsrq,
            ss_rsrp,
            ss_rsrq,
            cqi
    )
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, operator_name, NAME_1, NAME_2)
SETTINGS index_granularity = 8192;

insert into test select * from generateRandom() limit 10;

with tt as (
    select cast(toStartOfInterval(timestamp, INTERVAL 1 day) as Date) as dd, count() as samples
    from test
    group by dd having dd >= toDate(now())-100
    ),
tt2 as (
    select dd, samples from tt
    union distinct
    select toDate(now())-1, ifnull((select samples from tt where dd = toDate(now())-1),0) as samples
)
select dd, samples from tt2 order by dd with fill step 1 limit 100 format Null;

drop table test;
