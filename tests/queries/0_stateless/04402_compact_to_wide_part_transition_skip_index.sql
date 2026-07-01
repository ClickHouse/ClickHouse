-- A minmax skip index must keep working after a compact part becomes wide and is then mutated.
-- The compact->wide transition only happens during a merge (mutations inherit the source part
-- format), so it is reproduced with OPTIMIZE FINAL. The mutation that follows is what exercises
-- the path fixed in MutateTask: an UPDATE of a Dynamic column rewrites all columns of the part
-- (the all-columns mutation path), while the minmax index on the untouched column `v` is not
-- recalculated, so its files are hardlinked from the source part. The all-columns path rebuilds
-- checksums.txt from scratch, so the hardlinked index files must carry their source checksums;
-- otherwise the index stops gating granules and CHECK TABLE fails. A plain UPDATE of an ordinary
-- column would route to the some-columns path and not exercise the fix.

SET allow_experimental_dynamic_type = 1;

DROP TABLE IF EXISTS t_compact_to_wide;

CREATE TABLE t_compact_to_wide
(
    id UInt64,
    v UInt64,
    d Dynamic,
    INDEX m_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = '1G',
         min_rows_for_wide_part = 100000000,
         index_granularity = 1024;

INSERT INTO t_compact_to_wide SELECT number, number * 7, number::Dynamic FROM numbers(1000);
INSERT INTO t_compact_to_wide SELECT number + 1000, (number + 1000) * 7, number::Dynamic FROM numbers(1000);

SELECT 'before', arraySort(groupUniqArray(part_type)) FROM system.parts
    WHERE database = currentDatabase() AND table = 't_compact_to_wide' AND active;

ALTER TABLE t_compact_to_wide MODIFY SETTING min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
OPTIMIZE TABLE t_compact_to_wide FINAL;

SELECT 'after_merge', arraySort(groupUniqArray(part_type)) FROM system.parts
    WHERE database = currentDatabase() AND table = 't_compact_to_wide' AND active;

ALTER TABLE t_compact_to_wide UPDATE d = (id + 1)::Dynamic WHERE 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_compact_to_wide UPDATE v = v WHERE 1 SETTINGS mutations_sync = 2;

SELECT 'after_mutate', arraySort(groupUniqArray(part_type)) FROM system.parts
    WHERE database = currentDatabase() AND table = 't_compact_to_wide' AND active;

SELECT 'rows', count() FROM t_compact_to_wide;
SELECT 'filtered', countIf(toUInt64(g[1]) < toUInt64(g[2]))
FROM (
    SELECT extractGroups(explain, 'Granules: (\\d+)/(\\d+)') AS g
    FROM (EXPLAIN indexes = 1 SELECT * FROM t_compact_to_wide WHERE v = 700)
    WHERE match(explain, 'Granules: \\d+/\\d+') AND explain NOT LIKE '%PrimaryKey%'
) SETTINGS allow_experimental_analyzer = 1;
CHECK TABLE t_compact_to_wide SETTINGS check_query_single_value_result = 1;

DROP TABLE t_compact_to_wide;
