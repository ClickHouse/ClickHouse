DROP TABLE IF EXISTS with_lonely;

CREATE TABLE with_lonely
(
    `id` UInt64,
    `dt` Date,
    `val` UInt64,
    `version` UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY dt
ORDER BY (id);

INSERT INTO with_lonely SELECT number, '2022-10-28', number*10, 0 FROM numbers(10000);
INSERT INTO with_lonely SELECT number+500000, '2022-10-28', number*10, 1 FROM numbers(10000);
OPTIMIZE TABLE with_lonely PARTITION '2022-10-28' FINAL;


INSERT INTO with_lonely SELECT number, '2022-10-29', number*10, 0 FROM numbers(10000);
INSERT INTO with_lonely SELECT number+500000, '2022-10-29', number*10, 1 FROM numbers(10000);
OPTIMIZE TABLE with_lonely PARTITION '2022-10-29' FINAL;

INSERT INTO with_lonely SELECT number, '2022-10-30', number*10, 0 FROM numbers(10000);
INSERT INTO with_lonely SELECT number+500000, '2022-10-30', number*10, 1 FROM numbers(10000);
OPTIMIZE TABLE with_lonely PARTITION '2022-10-30' FINAL;


INSERT INTO with_lonely SELECT number, '2022-10-31', number*10, 0 FROM numbers(10000);
INSERT INTO with_lonely SELECT number+500000, '2022-10-31', number*10, 1 FROM numbers(10000);
OPTIMIZE TABLE with_lonely PARTITION '2022-10-31' FINAL;

SYSTEM STOP MERGES with_lonely;
INSERT INTO with_lonely SELECT number, '2022-11-01', number*10, 0 FROM numbers(1000);
INSERT INTO with_lonely SELECT number+50000, '2022-11-01', number*10, 1 FROM numbers(1000);
INSERT INTO with_lonely SELECT number+60000, '2022-11-01', number*10, 2 FROM numbers(1000);

SET do_not_merge_across_partitions_select_final = 1, max_threads = 16;

-- mix lonely parts and non-lonely parts
EXPLAIN PIPELINE header=1 SELECT max(val), count(*) FROM with_lonely FINAL SETTINGS allow_experimental_analyzer = 0;
-- only lonely parts
EXPLAIN PIPELINE header=1 SELECT max(val), count(*) FROM with_lonely FINAL WHERE dt < '2022-11-01' SETTINGS allow_experimental_analyzer = 0;
-- only lonely parts but max_thread = 1, so reading lonely parts with in-order
EXPLAIN PIPELINE header=1 SELECT max(val), count(*) FROM with_lonely FINAL WHERE dt < '2022-11-01' SETTINGS max_threads = 1, allow_experimental_analyzer = 0;


EXPLAIN PIPELINE header=1 SELECT max(val), count(*) FROM with_lonely FINAL SETTINGS allow_experimental_analyzer = 1;
EXPLAIN PIPELINE header=1 SELECT max(val), count(*) FROM with_lonely FINAL WHERE dt < '2022-11-01' SETTINGS allow_experimental_analyzer = 1;
EXPLAIN PIPELINE header=1 SELECT max(val), count(*) FROM with_lonely FINAL WHERE dt < '2022-11-01' SETTINGS max_threads = 1, allow_experimental_analyzer = 1;
