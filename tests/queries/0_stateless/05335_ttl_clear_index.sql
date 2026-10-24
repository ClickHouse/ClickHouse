-- `TTL ... CLEAR INDEX` with `ttl_clear_index_merges = 0`. Only regular merges clear indexes here.

DROP TABLE IF EXISTS ttl_clear_index;
DROP TABLE IF EXISTS ttl_clear_index_same_expr;
DROP TABLE IF EXISTS ttl_clear_index_update;

CREATE TABLE ttl_clear_index_bad (d Date, k UInt64, INDEX idx k TYPE minmax)
ENGINE = MergeTree ORDER BY k
TTL d + INTERVAL 1 DAY CLEAR INDEX missing_idx; -- { serverError BAD_ARGUMENTS }

CREATE TABLE ttl_clear_index_bad (d Date, k UInt64)
ENGINE = MergeTree ORDER BY k
TTL d + INTERVAL 1 DAY CLEAR INDEX auto_minmax_index_k
SETTINGS add_minmax_index_for_numeric_columns = 1; -- { serverError BAD_ARGUMENTS }

-- `idx_v` expires and `idx_w` has no rule.
CREATE TABLE ttl_clear_index
(
    d Date,
    k UInt64,
    v UInt64,
    w UInt64,
    INDEX idx_v v TYPE minmax GRANULARITY 1,
    INDEX idx_w w TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
TTL d + INTERVAL 1 DAY CLEAR INDEX idx_v
SETTINGS
    ttl_clear_index_merges = 0,
    index_granularity = 2,
    index_granularity_bytes = '10Mi',
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0;

INSERT INTO ttl_clear_index SELECT if(number < 10, '2000-06-01', '2001-06-01'), number, number, number FROM numbers(20);

SELECT index_clear_ttl_info.expression, toYear(index_clear_ttl_info.min[1]), toYear(index_clear_ttl_info.max[1])
FROM system.parts WHERE database = currentDatabase() AND table = 'ttl_clear_index' AND active;

-- Without `FINAL`, `OPTIMIZE` selects TTL merges. `TTLClearIndex` merges are off, so the part stays as it is.
OPTIMIZE TABLE ttl_clear_index;
SELECT name, level FROM system.parts WHERE database = currentDatabase() AND table = 'ttl_clear_index' AND active;
SELECT name, data_compressed_bytes > 0 FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 'ttl_clear_index' ORDER BY name;

-- A regular merge leaves only the expired index out.
OPTIMIZE TABLE ttl_clear_index FINAL;
SELECT name, data_compressed_bytes > 0 FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 'ttl_clear_index' ORDER BY name;

-- A query on `v` reads every granule now that `idx_v` is gone. `idx_w` still skips granules.
SELECT count() FROM ttl_clear_index WHERE v = 7;
SELECT count() FROM ttl_clear_index WHERE w = 7 SETTINGS max_rows_to_read = 2;
SELECT count() FROM ttl_clear_index WHERE v = 7 SETTINGS max_rows_to_read = 2; -- { serverError TOO_MANY_ROWS }

-- `MATERIALIZE INDEX` writes the index again, and the next merge removes it again.
ALTER TABLE ttl_clear_index MATERIALIZE INDEX idx_v SETTINGS mutations_sync = 2;
SELECT data_compressed_bytes > 0 FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 'ttl_clear_index' AND name = 'idx_v';
OPTIMIZE TABLE ttl_clear_index FINAL;
SELECT data_compressed_bytes > 0 FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 'ttl_clear_index' AND name = 'idx_v';

CHECK TABLE ttl_clear_index SETTINGS check_query_single_value_result = 1;

-- Two rules with the same TTL expression share one TTL info entry and both indexes are cleared. The index
-- names need quoting, so this also checks that `CLEAR INDEX` formats back to SQL that parses.
CREATE TABLE ttl_clear_index_same_expr
(
    d Date,
    k UInt64,
    v UInt64,
    INDEX `my-idx` v TYPE minmax GRANULARITY 1,
    INDEX `select` k TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
TTL d + INTERVAL 1 DAY CLEAR INDEX `my-idx`, d + INTERVAL 1 DAY CLEAR INDEX `select`
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO ttl_clear_index_same_expr VALUES ('2000-01-01', 1, 1), ('2000-01-01', 2, 2);
SELECT length(index_clear_ttl_info.expression)
FROM system.parts WHERE database = currentDatabase() AND table = 'ttl_clear_index_same_expr' AND active;
OPTIMIZE TABLE ttl_clear_index_same_expr FINAL;
SELECT name, data_compressed_bytes FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 'ttl_clear_index_same_expr' ORDER BY name;

-- An `UPDATE` of the TTL column recalculates the TTL info, so the index is no longer expired.
CREATE TABLE ttl_clear_index_update
(
    d Date,
    k UInt64,
    INDEX idx k TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
TTL d + INTERVAL 1 DAY CLEAR INDEX idx
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO ttl_clear_index_update VALUES ('2000-01-01', 1), ('2000-01-01', 2);
ALTER TABLE ttl_clear_index_update UPDATE d = '2100-01-01' WHERE 1 SETTINGS mutations_sync = 2;
SELECT toYear(index_clear_ttl_info.max[1])
FROM system.parts WHERE database = currentDatabase() AND table = 'ttl_clear_index_update' AND active;
OPTIMIZE TABLE ttl_clear_index_update FINAL;
SELECT data_compressed_bytes > 0 FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 'ttl_clear_index_update';

DROP TABLE ttl_clear_index;
DROP TABLE ttl_clear_index_same_expr;
DROP TABLE ttl_clear_index_update;
