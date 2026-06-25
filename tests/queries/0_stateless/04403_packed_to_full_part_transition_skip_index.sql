DROP TABLE IF EXISTS t_packed_to_full;

CREATE TABLE t_packed_to_full
(
    id UInt64,
    v UInt64,
    INDEX m_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_full_part_storage = '1G',
         min_bytes_for_wide_part = '1G',
         min_rows_for_wide_part = 100000000,
         index_granularity = 1024;

INSERT INTO t_packed_to_full SELECT number, number * 7 FROM numbers(1000);
INSERT INTO t_packed_to_full SELECT number + 1000, (number + 1000) * 7 FROM numbers(1000);

SELECT 'before', arraySort(groupUniqArray(part_storage_type)) FROM system.parts
    WHERE database = currentDatabase() AND table = 't_packed_to_full' AND active;

ALTER TABLE t_packed_to_full MODIFY SETTING min_bytes_for_full_part_storage = 0;
OPTIMIZE TABLE t_packed_to_full FINAL;

SELECT 'after', arraySort(groupUniqArray(part_storage_type)) FROM system.parts
    WHERE database = currentDatabase() AND table = 't_packed_to_full' AND active;

SELECT 'rows', count() FROM t_packed_to_full;
SELECT 'filtered', countIf(
    splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[1]::UInt64
    < splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[2]::UInt64)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_packed_to_full WHERE v = 700) AS s
WHERE explain LIKE '%Granules:%' AND explain NOT LIKE '%PrimaryKey%' SETTINGS allow_experimental_analyzer = 1;
CHECK TABLE t_packed_to_full SETTINGS check_query_single_value_result = 1;

DROP TABLE t_packed_to_full;
