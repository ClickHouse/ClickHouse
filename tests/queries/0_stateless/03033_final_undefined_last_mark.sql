-- Tags: no-random-settings, no-random-merge-tree-settings

CREATE TABLE account_test
(
    `id` UInt64,
    `row_ver` UInt64,
)
ENGINE = ReplacingMergeTree(row_ver)
PARTITION BY id % 64
ORDER BY id
SETTINGS index_granularity = 512, index_granularity_bytes = 0,
         min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0,
         min_rows_for_compact_part = 0, min_bytes_for_compact_part = 0;

INSERT INTO account_test
	SELECT * FROM generateRandom('id UInt64, row_ver UInt64',1234) LIMIT 50000;

INSERT INTO account_test
    SELECT * FROM (SELECT * FROM generateRandom('id UInt64, row_ver UInt64',1234) LIMIT 1000) WHERE row_ver > 14098131981223776000;

SELECT 'GOOD', * FROM account_test FINAL WHERE id = 11338881281426660955 SETTINGS split_parts_ranges_into_intersecting_and_non_intersecting_final = 1;
