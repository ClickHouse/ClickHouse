DROP TABLE IF EXISTS t_drop_prefix_a;
CREATE TABLE t_drop_prefix_a
(
    id UInt64,
    v UInt64,
    w UInt64,
    INDEX `a` v TYPE minmax GRANULARITY 1,
    INDEX `a.b` w TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS escape_index_filenames = 0,
         min_bytes_for_wide_part = 0,
         packed_skip_index_max_bytes = '1M',
         auto_statistics_types = '',
         index_granularity = 1024;

INSERT INTO t_drop_prefix_a SELECT number, number * 2, number * 3 FROM numbers(2000);

ALTER TABLE t_drop_prefix_a DROP INDEX `a` SETTINGS mutations_sync = 2;

SELECT 'drop_a_query_ab', count() FROM t_drop_prefix_a WHERE w BETWEEN 100 AND 200;
SELECT 'drop_a_ab_filtered', countIf(
    splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[1]::UInt64
    < splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[2]::UInt64)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_drop_prefix_a WHERE w = 250) AS s
WHERE explain LIKE '%Granules:%' AND explain NOT LIKE '%PrimaryKey%' SETTINGS allow_experimental_analyzer = 1;
CHECK TABLE t_drop_prefix_a SETTINGS check_query_single_value_result = 1;
DROP TABLE t_drop_prefix_a;

DROP TABLE IF EXISTS t_drop_prefix_ab;
CREATE TABLE t_drop_prefix_ab
(
    id UInt64,
    v UInt64,
    w UInt64,
    INDEX `a` v TYPE minmax GRANULARITY 1,
    INDEX `a.b` w TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS escape_index_filenames = 0,
         min_bytes_for_wide_part = 0,
         packed_skip_index_max_bytes = '1M',
         auto_statistics_types = '',
         index_granularity = 1024;

INSERT INTO t_drop_prefix_ab SELECT number, number * 2, number * 3 FROM numbers(2000);

ALTER TABLE t_drop_prefix_ab DROP INDEX `a.b` SETTINGS mutations_sync = 2;

SELECT 'drop_ab_query_a', count() FROM t_drop_prefix_ab WHERE v BETWEEN 100 AND 200;
SELECT 'drop_ab_a_filtered', countIf(
    splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[1]::UInt64
    < splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[2]::UInt64)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_drop_prefix_ab WHERE v = 250) AS s
WHERE explain LIKE '%Granules:%' AND explain NOT LIKE '%PrimaryKey%' SETTINGS allow_experimental_analyzer = 1;
CHECK TABLE t_drop_prefix_ab SETTINGS check_query_single_value_result = 1;
DROP TABLE t_drop_prefix_ab;

DROP TABLE IF EXISTS t_mutate_prefix;
CREATE TABLE t_mutate_prefix
(
    id UInt64,
    v UInt64,
    w UInt64
)
ENGINE = MergeTree
ORDER BY id
SETTINGS escape_index_filenames = 0,
         min_bytes_for_full_part_storage = '1G',
         min_bytes_for_wide_part = '1G',
         min_rows_for_wide_part = 100000000,
         auto_statistics_types = '',
         index_granularity = 1024;

INSERT INTO t_mutate_prefix SELECT number, number * 2, number * 3 FROM numbers(2000);

SELECT 'mutate_prefix_storage', part_type, part_storage_type FROM system.parts
    WHERE database = currentDatabase() AND table = 't_mutate_prefix' AND active;

ALTER TABLE t_mutate_prefix ADD INDEX `a` v TYPE minmax GRANULARITY 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_mutate_prefix ADD INDEX `a.b` w TYPE minmax GRANULARITY 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_mutate_prefix MATERIALIZE INDEX `a.b` SETTINGS mutations_sync = 2;

ALTER TABLE t_mutate_prefix UPDATE w = w + 1 WHERE id < 100 SETTINGS mutations_sync = 2;

SELECT 'mutate_prefix_rows', count() FROM t_mutate_prefix;
SELECT 'mutate_prefix_ab_filtered', countIf(
    splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[1]::UInt64
    < splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[2]::UInt64)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_mutate_prefix WHERE w = 250) AS s
WHERE explain LIKE '%Granules:%' AND explain NOT LIKE '%PrimaryKey%' SETTINGS allow_experimental_analyzer = 1;
CHECK TABLE t_mutate_prefix SETTINGS check_query_single_value_result = 1;
DROP TABLE t_mutate_prefix;
