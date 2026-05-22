-- Granule-level pruning in `data_read` (lazy) mode: the classification runs at
-- scan time via `MergeTreeSparsityReader` and feeds `MergeTreeReaderIndex::canSkipMark`
-- (same path as `use_skip_indexes_on_data_read`). Result must match the `off` baseline.

DROP TABLE IF EXISTS t_granule_data_read;

CREATE TABLE t_granule_data_read
(
    id UInt64,
    x UInt32
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 100, ratio_of_defaults_for_sparse_serialization = 0.5;

SYSTEM STOP MERGES t_granule_data_read;

INSERT INTO t_granule_data_read SELECT number, if(number % 200 = 0, 1, 0) FROM numbers(1000)
SETTINGS optimize_on_insert = 0;

SELECT serialization_kind FROM system.parts_columns
WHERE table = 't_granule_data_read' AND database = currentDatabase() AND column = 'x';

SET optimize_trivial_count_with_sparsity_filter = 0;

SELECT 'baseline countIf', countIf(x != 0) FROM t_granule_data_read;

SELECT 'data_read count', count() FROM t_granule_data_read WHERE x != 0 SETTINGS use_sparsity_info_for_pruning = 'data_read';
SELECT 'data_read sum',   sum(x)  FROM t_granule_data_read WHERE x != 0 SETTINGS use_sparsity_info_for_pruning = 'data_read';

-- Cross-check against the other modes -- result must be identical.
SELECT 'off count',       count() FROM t_granule_data_read WHERE x != 0 SETTINGS use_sparsity_info_for_pruning = 'off';
SELECT 'planning count',  count() FROM t_granule_data_read WHERE x != 0 SETTINGS use_sparsity_info_for_pruning = 'planning';

-- Direction that can't be pruned: no granule is all-non-default, so nothing drops.
SELECT 'data_read x=0', count() FROM t_granule_data_read WHERE x = 0 SETTINGS use_sparsity_info_for_pruning = 'data_read';

DROP TABLE t_granule_data_read;
