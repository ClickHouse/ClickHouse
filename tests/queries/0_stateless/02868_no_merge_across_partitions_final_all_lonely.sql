DROP TABLE IF EXISTS all_lonely;

CREATE TABLE all_lonely
(
    `id` UInt64,
    `dt` Date,
    `val` UInt64,
    `version` UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY dt
ORDER BY (id);

INSERT INTO all_lonely SELECT number, '2022-10-28', number*10, 0 FROM numbers(10000);
INSERT INTO all_lonely SELECT number+500000, '2022-10-28', number*10, 1 FROM numbers(10000);
OPTIMIZE TABLE all_lonely PARTITION '2022-10-28' FINAL;


INSERT INTO all_lonely SELECT number, '2022-10-29', number*10, 0 FROM numbers(10000);
INSERT INTO all_lonely SELECT number+500000, '2022-10-29', number*10, 1 FROM numbers(10000);
OPTIMIZE TABLE all_lonely PARTITION '2022-10-29' FINAL;

INSERT INTO all_lonely SELECT number, '2022-10-30', number*10, 0 FROM numbers(10000);
INSERT INTO all_lonely SELECT number+500000, '2022-10-30', number*10, 1 FROM numbers(10000);
OPTIMIZE TABLE all_lonely PARTITION '2022-10-30' FINAL;


INSERT INTO all_lonely SELECT number, '2022-10-31', number*10, 0 FROM numbers(10000);
INSERT INTO all_lonely SELECT number+500000, '2022-10-31', number*10, 1 FROM numbers(10000);
OPTIMIZE TABLE all_lonely PARTITION '2022-10-31' FINAL;

EXPLAIN PIPELINE header=1 SELECT max(val), count(*) FROM all_lonely FINAL SETTINGS do_not_merge_across_partitions_select_final = 1, max_threads = 16;
