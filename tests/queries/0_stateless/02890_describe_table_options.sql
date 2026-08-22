DROP TABLE IF EXISTS t_describe_options;

SET print_pretty_type_names = 0;

CREATE TABLE t_describe_options (
    id UInt64 COMMENT 'index column',
    arr Array(UInt64) DEFAULT [10, 20] CODEC(ZSTD),
    t Tuple(a String, b UInt64) DEFAULT ('foo', 0) CODEC(ZSTD))
ENGINE = MergeTree
ORDER BY id;

-- { echoOn }

SET describe_compact_output = 0, describe_include_virtual_columns = 0, describe_include_subcolumns = 0;

DESCRIBE TABLE t_describe_options;
DESCRIBE remote(test_shard_localhost, currentDatabase(), t_describe_options);

SET describe_compact_output = 0, describe_include_virtual_columns = 0, describe_include_subcolumns = 1;

DESCRIBE TABLE t_describe_options;
DESCRIBE remote(test_shard_localhost, currentDatabase(), t_describe_options);

SET describe_compact_output = 0, describe_include_virtual_columns = 1, describe_include_subcolumns = 0;

DESCRIBE TABLE t_describe_options;
DESCRIBE remote(test_shard_localhost, currentDatabase(), t_describe_options);

SET describe_compact_output = 0, describe_include_virtual_columns = 1, describe_include_subcolumns = 1;

DESCRIBE TABLE t_describe_options;
DESCRIBE remote(test_shard_localhost, currentDatabase(), t_describe_options);

SET describe_compact_output = 1, describe_include_virtual_columns = 0, describe_include_subcolumns = 0;

DESCRIBE TABLE t_describe_options;
DESCRIBE remote(test_shard_localhost, currentDatabase(), t_describe_options);

SET describe_compact_output = 1, describe_include_virtual_columns = 0, describe_include_subcolumns = 1;

DESCRIBE TABLE t_describe_options;
DESCRIBE remote(test_shard_localhost, currentDatabase(), t_describe_options);

SET describe_compact_output = 1, describe_include_virtual_columns = 1, describe_include_subcolumns = 0;

DESCRIBE TABLE t_describe_options;
DESCRIBE remote(test_shard_localhost, currentDatabase(), t_describe_options);

SET describe_compact_output = 1, describe_include_virtual_columns = 1, describe_include_subcolumns = 1;

DESCRIBE TABLE t_describe_options;
DESCRIBE remote(test_shard_localhost, currentDatabase(), t_describe_options);

-- { echoOff }

DROP TABLE t_describe_options;
