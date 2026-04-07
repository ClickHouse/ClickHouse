DROP TABLE IF EXISTS fx_1m;
DROP TABLE IF EXISTS fx_5m;

-- create source table
CREATE TABLE fx_1m (
    `symbol` LowCardinality(String) CODEC(ZSTD),
    `dt_close` DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD),
    `open` Float32 CODEC(Delta, ZSTD),
    `high` Float32 CODEC(Delta, ZSTD),
    `low` Float32 CODEC(Delta, ZSTD),
    `close` Float32 CODEC(Delta, ZSTD),
    `volume` Float32 CODEC(Delta, ZSTD)
)
ENGINE = MergeTree()
PARTITION BY toYear(dt_close)
ORDER BY (symbol, dt_close) SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

-- add projection
ALTER TABLE fx_1m
ADD PROJECTION fx_5m (
    SELECT
        symbol,
        toStartOfInterval(dt_close, INTERVAL 300 SECOND) AS dt_close,
        argMin(open, dt_close),
        max(high),
        min(low),
        argMax(close, dt_close),
        sum(volume) volume
    GROUP BY symbol, dt_close
);

-- materialize projection
ALTER TABLE fx_1m MATERIALIZE PROJECTION fx_5m SETTINGS mutations_sync = 2;

-- create view using projection
CREATE VIEW fx_5m AS
SELECT
    symbol,
    toStartOfInterval(dt_close, INTERVAL 300 SECOND) AS dt_close,
    argMin(open, dt_close) open,
    max(high) high,
    min(low) low,
    argMax(close, dt_close) close,
    sum(volume) volume
FROM fx_1m
GROUP BY symbol, dt_close;

-- insert sample data
INSERT INTO fx_1m
SELECT
    'EURUSD',
    toDateTime64('2022-12-12 12:00:00', 3, 'UTC') + number,
    number + randCanonical(),
    number + randCanonical(),
    number + randCanonical(),
    number + randCanonical(),
    number + randCanonical()
FROM numbers(1000000);

-- segmentation fault (filter on dt_close column)
SELECT
    dt_close,
    close
FROM fx_5m
where symbol = 'EURUSD' and dt_close between '2022-12-11' and '2022-12-13'
order by dt_close
format Null;

DROP TABLE fx_5m;
DROP TABLE fx_1m;
