-- An implicit minmax index is named after its column, and the on-disk file name of a skip index is
-- never replaced by a hash the way a column data file is (`replace_long_file_name_to_hash`). A
-- numeric column whose name does not fit into a file name once `skp_idx_auto_minmax_index_` is
-- prepended therefore must not get an implicit index, otherwise every `INSERT` into the table fails
-- with `File name too long`.

DROP TABLE IF EXISTS t_implicit_minmax_long_name;

-- 8 (`skp_idx_`) + 18 (`auto_minmax_index_`) + 220 exceeds the file name limit.
CREATE TABLE t_implicit_minmax_long_name
(
    `cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc` UInt64
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1, packed_skip_index_max_bytes = 0;

INSERT INTO t_implicit_minmax_long_name VALUES (1);

SELECT count() FROM t_implicit_minmax_long_name;
SELECT count() FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_implicit_minmax_long_name';

DROP TABLE t_implicit_minmax_long_name;

-- A name that still fits keeps its implicit index, and the part is written.
DROP TABLE IF EXISTS t_implicit_minmax_fitting_name;

CREATE TABLE t_implicit_minmax_fitting_name
(
    `cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc` UInt64
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1, packed_skip_index_max_bytes = 0;

INSERT INTO t_implicit_minmax_fitting_name VALUES (1);

SELECT count() FROM t_implicit_minmax_fitting_name;
SELECT count() FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_implicit_minmax_fitting_name';

DROP TABLE t_implicit_minmax_fitting_name;

-- A non-ASCII name is escaped into the file name, so three bytes per character become nine, and a
-- name that is short in characters can still overflow.
DROP TABLE IF EXISTS t_implicit_minmax_escaped_name;

CREATE TABLE t_implicit_minmax_escaped_name
(
    `一个非常非常非常非常非常非常非常非常非常非常非常长的中文字符串` Int
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1, packed_skip_index_max_bytes = 0,
         replace_long_file_name_to_hash = 1, max_file_name_length = 127;

INSERT INTO t_implicit_minmax_escaped_name VALUES (1);

SELECT count() FROM t_implicit_minmax_escaped_name;
SELECT count() FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_implicit_minmax_escaped_name';

DROP TABLE t_implicit_minmax_escaped_name;
