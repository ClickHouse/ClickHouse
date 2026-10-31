-- While a `RENAME COLUMN` or a `DROP COLUMN` followed by re-adding a column with the same name is
-- still pending, reads already apply it, but the implicit minmax index of a part is found by a name
-- derived from the column name. The index file found that way then describes another column's data,
-- or the data of the dropped column whose reads are filled with the new default, so it must not be
-- used to skip granules of such a part.

SET use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_skip_index_pending_readd;
CREATE TABLE t_skip_index_pending_readd (id Int64, x Int64) ENGINE = MergeTree ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 1, index_granularity = 8192;
-- Only to hold the mutation pending deterministically.
SYSTEM STOP MERGES t_skip_index_pending_readd;
INSERT INTO t_skip_index_pending_readd VALUES (1, 1), (2, 2);

ALTER TABLE t_skip_index_pending_readd DROP COLUMN x, ADD COLUMN x Int64 DEFAULT 100 SETTINGS alter_sync = 0, mutations_sync = 0;

SELECT groupArray(x) FROM t_skip_index_pending_readd;
SELECT count(), (SELECT count() FROM t_skip_index_pending_readd WHERE x = 100 SETTINGS use_skip_indexes = 0) FROM t_skip_index_pending_readd WHERE x = 100 SETTINGS use_skip_indexes = 1;
-- A value inside the dropped column's range must not match either.
SELECT count() FROM t_skip_index_pending_readd WHERE x = 1;

-- The top-k path must not trust the stale minmax either: the pending part advertises a maximum of 2,
-- which would displace the part that holds the real maximum.
INSERT INTO t_skip_index_pending_readd VALUES (3, 50);
-- With `use_skip_indexes_on_data_read = 0` the granules are selected before reading, otherwise
-- they are also compared against the threshold reached by the other part while reading.
SELECT x FROM t_skip_index_pending_readd ORDER BY x DESC LIMIT 1 SETTINGS use_skip_indexes_for_top_k = 1, use_top_k_dynamic_filtering = 1, use_skip_indexes_on_data_read = 0;
SELECT x FROM t_skip_index_pending_readd ORDER BY x ASC LIMIT 1 SETTINGS use_skip_indexes_for_top_k = 1, use_top_k_dynamic_filtering = 1, use_skip_indexes_on_data_read = 0;
SELECT x FROM t_skip_index_pending_readd ORDER BY x DESC LIMIT 1 SETTINGS use_skip_indexes_for_top_k = 1, use_top_k_dynamic_filtering = 1, use_skip_indexes_on_data_read = 1;
SELECT x FROM t_skip_index_pending_readd ORDER BY x ASC LIMIT 1 SETTINGS use_skip_indexes_for_top_k = 1, use_top_k_dynamic_filtering = 1, use_skip_indexes_on_data_read = 1;

SYSTEM START MERGES t_skip_index_pending_readd;
DROP TABLE t_skip_index_pending_readd;

-- Dropping `x` and renaming `y` to `x` makes the index named after `x` describe the dropped data.
DROP TABLE IF EXISTS t_skip_index_pending_rename;
CREATE TABLE t_skip_index_pending_rename (id Int64, x Int64, y Int64) ENGINE = MergeTree ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 1, index_granularity = 8192;
SYSTEM STOP MERGES t_skip_index_pending_rename;
INSERT INTO t_skip_index_pending_rename VALUES (1, 1, 100), (2, 2, 200);

ALTER TABLE t_skip_index_pending_rename DROP COLUMN x, RENAME COLUMN y TO x SETTINGS alter_sync = 0, mutations_sync = 0;

SELECT groupArray(x) FROM t_skip_index_pending_rename;
SELECT count(), (SELECT count() FROM t_skip_index_pending_rename WHERE x = 100 SETTINGS use_skip_indexes = 0) FROM t_skip_index_pending_rename WHERE x = 100 SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_skip_index_pending_rename WHERE x = 1;

SYSTEM START MERGES t_skip_index_pending_rename;
DROP TABLE t_skip_index_pending_rename;
