DROP TABLE IF EXISTS t_describe_options;

CREATE TABLE t_describe_options (
    id UInt64 COMMENT 'index column',
    arr Array(UInt64) DEFAULT [10, 20] CODEC(ZSTD),
    t Tuple(a String, b UInt64) DEFAULT ('foo', 0) CODEC(ZSTD))
ENGINE = MergeTree
ORDER BY id;

-- { echoOn }

SET describe_compact_output = 0, describe_include_virtual_columns = 0, describe_include_subcolumns = 0;

DESCRIBE TABLE t_describe_options FORMAT PrettyCompactNoEscapes;
DESCRIBE remote(default, currentDatabase(), t_describe_options) FORMAT PrettyCompactNoEscapes;

SET describe_compact_output = 0, describe_include_virtual_columns = 0, describe_include_subcolumns = 1;

DESCRIBE TABLE t_describe_options FORMAT PrettyCompactNoEscapes;
DESCRIBE remote(default, currentDatabase(), t_describe_options) FORMAT PrettyCompactNoEscapes;

SET describe_compact_output = 0, describe_include_virtual_columns = 1, describe_include_subcolumns = 0;

DESCRIBE TABLE t_describe_options FORMAT PrettyCompactNoEscapes;
DESCRIBE remote(default, currentDatabase(), t_describe_options) FORMAT PrettyCompactNoEscapes;

SET describe_compact_output = 0, describe_include_virtual_columns = 1, describe_include_subcolumns = 1;

DESCRIBE TABLE t_describe_options FORMAT PrettyCompactNoEscapes;
DESCRIBE remote(default, currentDatabase(), t_describe_options) FORMAT PrettyCompactNoEscapes;

SET describe_compact_output = 1, describe_include_virtual_columns = 0, describe_include_subcolumns = 0;

DESCRIBE TABLE t_describe_options FORMAT PrettyCompactNoEscapes;
DESCRIBE remote(default, currentDatabase(), t_describe_options) FORMAT PrettyCompactNoEscapes;

SET describe_compact_output = 1, describe_include_virtual_columns = 0, describe_include_subcolumns = 1;

DESCRIBE TABLE t_describe_options FORMAT PrettyCompactNoEscapes;
DESCRIBE remote(default, currentDatabase(), t_describe_options) FORMAT PrettyCompactNoEscapes;

SET describe_compact_output = 1, describe_include_virtual_columns = 1, describe_include_subcolumns = 0;

DESCRIBE TABLE t_describe_options FORMAT PrettyCompactNoEscapes;
DESCRIBE remote(default, currentDatabase(), t_describe_options) FORMAT PrettyCompactNoEscapes;

SET describe_compact_output = 1, describe_include_virtual_columns = 1, describe_include_subcolumns = 1;

DESCRIBE TABLE t_describe_options FORMAT PrettyCompactNoEscapes;
DESCRIBE remote(default, currentDatabase(), t_describe_options) FORMAT PrettyCompactNoEscapes;

-- { echoOff }

DROP TABLE t_describe_options;
