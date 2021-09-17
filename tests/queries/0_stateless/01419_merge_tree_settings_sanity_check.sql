DROP TABLE IF EXISTS mytable_local;

CREATE TABLE mytable_local
(
    created          DateTime,
    eventday         Date,
    user_id          UInt32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(eventday)
ORDER BY (eventday, user_id)
SETTINGS ratio_of_merges_with_the_maximum_size = 100; -- { serverError 36 }

DROP TABLE IF EXISTS mytable_local;
