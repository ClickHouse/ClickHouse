-- Skip indexes are used for OR-connected filter conditions with more than 32 atoms (`use_skip_indexes_for_disjunctions`).

SET use_query_condition_cache = 0;
SET use_skip_indexes_on_data_read = 0;
SET use_skip_indexes = 1;
SET use_skip_indexes_for_disjunctions = 1;
-- Keep the equalities as separate atoms instead of folding them into `IN`.
SET optimize_min_equality_disjunction_chain_length = 1000;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    v1 UInt32,
    v2 UInt32,
    INDEX v1_idx v1 TYPE minmax GRANULARITY 1,
    INDEX v2_idx v2 TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, add_minmax_index_for_numeric_columns = 0,
    min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

-- 157 granules
INSERT INTO tab SELECT number, number, 10000 - number FROM numbers(10000);
OPTIMIZE TABLE tab FINAL;

-- 40 atoms (79 RPN elements), each atom matches one row in a separate granule, so 40 granules (2560 rows) are read.
-- If the skip indexes are not combined, neither index can prune a granule on its own and all 10000 rows are read.
SELECT count() FROM tab
WHERE v1 = 250 OR v1 = 500 OR v1 = 750 OR v1 = 1000 OR v1 = 1250 OR v1 = 1500 OR v1 = 1750 OR v1 = 2000 OR v1 = 2250 OR v1 = 2500
   OR v1 = 2750 OR v1 = 3000 OR v1 = 3250 OR v1 = 3500 OR v1 = 3750 OR v1 = 4000 OR v1 = 4250 OR v1 = 4500 OR v1 = 4750 OR v1 = 5000
   OR v2 = 375 OR v2 = 625 OR v2 = 875 OR v2 = 1125 OR v2 = 1375 OR v2 = 1625 OR v2 = 1875 OR v2 = 2125 OR v2 = 2375 OR v2 = 2625
   OR v2 = 2875 OR v2 = 3125 OR v2 = 3375 OR v2 = 3625 OR v2 = 3875 OR v2 = 4125 OR v2 = 4375 OR v2 = 4625 OR v2 = 4875 OR v2 = 5125
SETTINGS max_rows_to_read = 2560, read_overflow_mode = 'throw';

-- The skip indexes are combined: 40 of 157 granules remain after the AND/OR processing.
SET explain_query_plan_default = 'legacy';
WITH
    (
        SELECT groupArray(trimBoth(explain)) FROM (
            EXPLAIN indexes = 1 SELECT count() FROM tab
            WHERE v1 = 250 OR v1 = 500 OR v1 = 750 OR v1 = 1000 OR v1 = 1250 OR v1 = 1500 OR v1 = 1750 OR v1 = 2000 OR v1 = 2250 OR v1 = 2500
               OR v1 = 2750 OR v1 = 3000 OR v1 = 3250 OR v1 = 3500 OR v1 = 3750 OR v1 = 4000 OR v1 = 4250 OR v1 = 4500 OR v1 = 4750 OR v1 = 5000
               OR v2 = 375 OR v2 = 625 OR v2 = 875 OR v2 = 1125 OR v2 = 1375 OR v2 = 1625 OR v2 = 1875 OR v2 = 2125 OR v2 = 2375 OR v2 = 2625
               OR v2 = 2875 OR v2 = 3125 OR v2 = 3375 OR v2 = 3625 OR v2 = 3875 OR v2 = 4125 OR v2 = 4375 OR v2 = 4625 OR v2 = 4875 OR v2 = 5125
        )
    ) AS lines,
    indexOf(lines, 'Name: <Combined skip indexes>') AS pos
SELECT lines[pos], lines[pos + 3];

DROP TABLE tab;
