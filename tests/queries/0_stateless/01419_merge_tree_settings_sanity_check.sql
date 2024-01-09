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
SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 100; -- { serverError 36 }

CREATE TABLE mytable_local
(
    created          DateTime,
    eventday         Date,
    user_id          UInt32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(eventday)
ORDER BY (eventday, user_id)
SETTINGS number_of_free_entries_in_pool_to_lower_max_size_of_merge = 100; -- { serverError 36 }

CREATE TABLE mytable_local
(
    created          DateTime,
    eventday         Date,
    user_id          UInt32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(eventday)
ORDER BY (eventday, user_id)
SETTINGS number_of_free_entries_in_pool_to_execute_optimize_entire_partition = 100; -- { serverError 36 }

CREATE TABLE mytable_local
(
    created          DateTime,
    eventday         Date,
    user_id          UInt32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(eventday)
ORDER BY (eventday, user_id);

ALTER TABLE mytable_local MODIFY SETTING number_of_free_entries_in_pool_to_execute_mutation = 100;  -- { serverError 36 }

DROP TABLE mytable_local;
