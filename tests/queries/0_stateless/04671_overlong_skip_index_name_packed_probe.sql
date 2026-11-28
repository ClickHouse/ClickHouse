-- Tags: no-random-merge-tree-settings
-- The test pins `replace_long_file_name_to_hash`, `max_file_name_length` and
-- `packed_skip_index_max_bytes`, which is exactly what randomization changes.

-- A skip index whose file name does not fit into a directory entry gets its substreams stored
-- under a hash instead (`replace_long_file_name_to_hash`). With skip-index packing enabled, the
-- existence probe for such a substream was still made under the logical, unhashed name, and
-- `stat` on a name no filesystem can represent throws instead of reporting the file as absent, so
-- every `INSERT` into the table failed with:
--     Code: 1001 ... in posix_stat: ... File name too long [".../skp_idx_<...>.idx"]
-- The implicit minmax index is named after its column, so a long - or merely non-ASCII, since
-- `escapeForFileName` turns every byte into three characters - column name hits this too.

DROP TABLE IF EXISTS t_overlong_explicit_index;

-- 8 (`skp_idx_`) + 260 exceeds the length of a directory entry.
CREATE TABLE t_overlong_explicit_index
(
    a UInt64,
    INDEX `eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee` a TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS replace_long_file_name_to_hash = 1, max_file_name_length = 127, packed_skip_index_max_bytes = 100;

INSERT INTO t_overlong_explicit_index VALUES (1), (2);

SELECT count() FROM t_overlong_explicit_index;
SELECT count() FROM t_overlong_explicit_index WHERE a = 1;

DROP TABLE t_overlong_explicit_index;

-- The same, for the implicit minmax index of a non-ASCII column name.

DROP TABLE IF EXISTS t_overlong_implicit_index;

CREATE TABLE t_overlong_implicit_index
(
    `一个非常非常非常非常非常非常非常非常非常非常非常长的中文字符串` UInt64
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1,
         replace_long_file_name_to_hash = 1, max_file_name_length = 127, packed_skip_index_max_bytes = 100;

INSERT INTO t_overlong_implicit_index VALUES (1), (2);

SELECT name FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_overlong_implicit_index';

SELECT count() FROM t_overlong_implicit_index;
SELECT count() FROM t_overlong_implicit_index WHERE `一个非常非常非常非常非常非常非常非常非常非常非常长的中文字符串` = 1;

DROP TABLE t_overlong_implicit_index;
