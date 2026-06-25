DROP TABLE IF EXISTS t_compact_to_wide;

CREATE TABLE t_compact_to_wide
(
    id UInt64,
    v UInt64,
    INDEX m_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = '1G',
         min_rows_for_wide_part = 100000000,
         index_granularity = 1024;

INSERT INTO t_compact_to_wide SELECT number, number * 7 FROM numbers(1000);
INSERT INTO t_compact_to_wide SELECT number + 1000, (number + 1000) * 7 FROM numbers(1000);

SELECT 'before', arraySort(groupUniqArray(part_type)) FROM system.parts
    WHERE database = currentDatabase() AND table = 't_compact_to_wide' AND active;

ALTER TABLE t_compact_to_wide MODIFY SETTING min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
OPTIMIZE TABLE t_compact_to_wide FINAL;

SELECT 'after', arraySort(groupUniqArray(part_type)) FROM system.parts
    WHERE database = currentDatabase() AND table = 't_compact_to_wide' AND active;

SELECT 'rows', count() FROM t_compact_to_wide;
SELECT 'filtered', countIf(
    splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[1]::UInt64
    < splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[2]::UInt64)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_compact_to_wide WHERE v = 700) AS s
WHERE explain LIKE '%Granules:%' AND explain NOT LIKE '%PrimaryKey%' SETTINGS allow_experimental_analyzer = 1;
CHECK TABLE t_compact_to_wide SETTINGS check_query_single_value_result = 1;

DROP TABLE t_compact_to_wide;
