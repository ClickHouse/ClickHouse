CREATE TABLE xx
(
    `date` Date, 
    `id` Int64, 
    `clicks` Int64, 
    `price` Float64, 
    `spend` Float64
)
ENGINE = SummingMergeTree([price, spend])
PARTITION BY toYYYYMM(date)
ORDER BY id
SAMPLE BY id
SETTINGS index_granularity = 8192; -- { serverError 223 }
