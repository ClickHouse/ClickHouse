-- The reading header of a FINAL query can hold two identically named columns: the sorting key
-- expression is computed for the merging transform and the same expression is also carried over from
-- PREWHERE. Splitting intersecting ranges into layers filters every layer by its primary key range,
-- and that filter must not change the stream header.

DROP TABLE IF EXISTS t_final_split_dup;

CREATE TABLE t_final_split_dup
(
    org_id UInt64,
    folder UUID,
    item UUID,
    sampling UInt16,
    is_deleted Bool,
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (toUInt128(folder), sampling, toUInt128(item))
SETTINGS index_granularity = 8;

SYSTEM STOP MERGES t_final_split_dup;

-- Three parts covering the same primary key range, so that reading FINAL is split into layers.
INSERT INTO t_final_split_dup SELECT 1, toUUID('00000000-0000-0000-0000-000000000004'), toUUIDOrZero(''), number % 64, 0, 1000 + number FROM numbers(100);
INSERT INTO t_final_split_dup SELECT 1, toUUID('00000000-0000-0000-0000-000000000004'), toUUIDOrZero(''), number % 64, 0, 2000 + number FROM numbers(100);
INSERT INTO t_final_split_dup SELECT 1, toUUID('00000000-0000-0000-0000-000000000004'), toUUIDOrZero(''), number % 64, 0, 3000 + number FROM numbers(100);

SET split_intersecting_parts_ranges_into_layers_final = 1;
SET do_not_merge_across_partitions_select_final = 0;
SET max_threads = 4;
SET max_final_threads = 4;

SET enable_analyzer = 1;

SELECT toUInt128(folder), sampling, toUInt128(item)
FROM t_final_split_dup FINAL
PREWHERE (org_id = 1) AND (modulo(toUInt128(folder), 4) = 0) AND (sampling < 512)
WHERE NOT is_deleted
ORDER BY toUInt128(folder), sampling, toUInt128(item)
LIMIT 5;

SELECT count()
FROM
(
    SELECT toUInt128(folder), sampling, toUInt128(item)
    FROM t_final_split_dup FINAL
    PREWHERE (org_id = 1) AND (modulo(toUInt128(folder), 4) = 0) AND (sampling < 512)
    WHERE NOT is_deleted
);

DROP TABLE t_final_split_dup;
