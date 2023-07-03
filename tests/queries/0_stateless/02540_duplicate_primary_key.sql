drop table if exists test;

set allow_suspicious_low_cardinality_types = 1;

CREATE TABLE test
(
    `coverage` DateTime,
    `haunt` Nullable(Float32) CODEC(Gorilla, ZSTD(1)),
    `sail` Nullable(Float32) CODEC(Gorilla, ZSTD(1)),
    `empowerment_turnstile` UInt8,
    `empowerment_haversack` Nullable(Int16),
    `empowerment_function` Nullable(Int16),
    `empowerment_guidance` Nullable(Int32),
    `empowerment_high` Nullable(Int32),
    `trading_id` Nullable(Int32),
    `guidance` Nullable(Int32),
    `empowerment_rawhide` Int32,
    `memo` Nullable(Int16),
    `oeuvre` Nullable(Int16),
    `bun` Nullable(Int16),
    `tramp` String,
    `anthropology_total` Nullable(Float32),
    `situation_name` String,
    `timing` Nullable(String),
    `NAME_cockroach` String,
    `NAME_toe` String,
    `business_error_methane` FixedString(110),
    `business_instrumentation_methane` FixedString(15),
    `market` UInt8,
    `crew_memo` Nullable(Int16),
    `crew_oeuvre` Nullable(Int16),
    `crew_fortnight` Nullable(Int16),
    `princess_memo` Nullable(Int16),
    `princess_oeuvre` Nullable(Int16),
    `princess_fortnight` Nullable(Int16),
    `emerald` Nullable(Float32),
    `cannon_crate` Nullable(String),
    `thinking` String,
    `SectorMen` String,
    `rage_name` Nullable(String),
    `DevelopmentalLigandName` String,
    `chard_heavy_quadrant` UInt64,
    `poster_effective` Nullable(String),
    PROJECTION chrysalis_trapezium_ham
    (
        SELECT
            empowerment_turnstile,
            toStartOfInterval(coverage, toIntervalMonth(1)),
            toStartOfWeek(coverage, 10),
            toStartOfInterval(coverage, toIntervalDay(1)),
            NAME_toe,
            NAME_cockroach,
            situation_name,
            memo,
            oeuvre,
            crew_memo,
            crew_oeuvre,
            bun,
            sum(multiIf(crew_memo IS NULL, 0, 1)),
            sum(multiIf(crew_oeuvre IS NULL, 0, 1)),
            sum(multiIf(crew_fortnight IS NULL, 0, 1)),
            max(toStartOfInterval(coverage, toIntervalDay(1))),
            max(CAST(CAST(toStartOfInterval(coverage, toIntervalDay(1)), 'Nullable(DATE)'), 'Nullable(TIMESTAMP)')),
            min(toStartOfInterval(coverage, toIntervalDay(1))),
            min(CAST(CAST(toStartOfInterval(coverage, toIntervalDay(1)), 'Nullable(DATE)'), 'Nullable(TIMESTAMP)')),
            count(),
            sum(1)
        GROUP BY
            empowerment_turnstile,
            toStartOfInterval(coverage, toIntervalMonth(1)),
            toStartOfWeek(coverage, 10),
            toStartOfInterval(coverage, toIntervalDay(1)),
            empowerment_turnstile,
            toStartOfInterval(coverage, toIntervalMonth(1)),
            toStartOfWeek(coverage, 10),
            toStartOfInterval(coverage, toIntervalDay(1)),
            NAME_toe,
            NAME_cockroach,
            situation_name,
            memo,
            oeuvre,
            crew_memo,
            crew_oeuvre,
            bun
    )
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(coverage)
ORDER BY (coverage, situation_name, NAME_toe, NAME_cockroach);

insert into test select * from generateRandom() limit 10;

with dissonance as (
    Select cast(toStartOfInterval(coverage, INTERVAL 1 day) as Date) as flour, count() as regulation
    from test
    group by flour having flour >= toDate(now())-100
    ),
cheetah as (
    Select flour, regulation from dissonance
    union distinct
    Select toDate(now())-1, ifnull((select regulation from dissonance where flour = toDate(now())-1),0) as regulation
)
Select flour, regulation from cheetah order by flour with fill step 1 limit 100 format Null;

drop table test;
