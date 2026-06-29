-- A mutation of a Dynamic column rewrites all columns of the part (the all-columns mutation
-- path). A per-file skip index on an unrelated column is not recalculated by such a mutation,
-- so its files are hardlinked from the source part into the new part. The all-columns path
-- rebuilds checksums.txt from scratch, so the hardlinked index files must have their source
-- checksums copied over; otherwise the new part has untracked files (CHECK TABLE fails) and the
-- index is no longer found via checksums (it stops gating granules).

SET allow_experimental_dynamic_type = 1;

DROP TABLE IF EXISTS t_mut_all_hardlink;

CREATE TABLE t_mut_all_hardlink
(
    id UInt64,
    v UInt64,
    d Dynamic,
    INDEX m_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1024;

INSERT INTO t_mut_all_hardlink SELECT number, number * 7, number::Dynamic FROM numbers(4000);

ALTER TABLE t_mut_all_hardlink UPDATE d = (id + 1)::Dynamic WHERE 1 SETTINGS mutations_sync = 2;

SELECT 'rows', count() FROM t_mut_all_hardlink;
SELECT 'filtered', countIf(toUInt64(g[1]) < toUInt64(g[2]))
FROM (
    SELECT extractGroups(explain, 'Granules: (\\d+)/(\\d+)') AS g
    FROM (EXPLAIN indexes = 1 SELECT * FROM t_mut_all_hardlink WHERE v = 700)
    WHERE match(explain, 'Granules: \\d+/\\d+') AND explain NOT LIKE '%PrimaryKey%'
) SETTINGS allow_experimental_analyzer = 1;
CHECK TABLE t_mut_all_hardlink SETTINGS check_query_single_value_result = 1;

DROP TABLE t_mut_all_hardlink;
