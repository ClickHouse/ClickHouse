-- Fast test skips `05054_hash_join_gather` because that file is tagged `long`.
-- This join checks `UInt64`, `String`, `Nullable(UInt64)` and `Array(UInt64)` against `full_sorting_merge`.
-- Fifty probe keys have no match, so the default row is part of the result.
-- The runner randomizes the join path settings, so this test pins them. The row store is pinned off so every payload column stays on the gather.

DROP TABLE IF EXISTS gj_build;
DROP TABLE IF EXISTS gj_probe;

CREATE TABLE gj_build
(
    k UInt64,
    u UInt64,
    s String,
    n Nullable(UInt64),
    a Array(UInt64)
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO gj_build
SELECT
    number,
    number * 1000003,
    if(number % 5 = 0, '', toString(number)),
    if(number % 7 = 0, NULL, number),
    range(number % 4)
FROM numbers(200);

CREATE TABLE gj_probe (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO gj_probe SELECT number FROM numbers(250);

SELECT 'hash', count(), sum(cityHash64(*))
FROM gj_probe LEFT JOIN gj_build USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

SELECT 'full_sorting_merge', count(), sum(cityHash64(*))
FROM gj_probe LEFT JOIN gj_build USING (k)
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

DROP TABLE gj_build;
DROP TABLE gj_probe;
