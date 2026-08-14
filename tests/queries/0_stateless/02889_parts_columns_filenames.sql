DROP TABLE IF EXISTS t_parts_columns_filenames;

CREATE TABLE t_parts_columns_filenames (id UInt64, v UInt64, long_v_name UInt64, long_arr_name Array(UInt64), arr_col Array(UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    replace_long_file_name_to_hash = 1,
    max_file_name_length = 8,
    ratio_of_defaults_for_sparse_serialization = 0.9;

INSERT INTO t_parts_columns_filenames SELECT number, 0, 0, range(number % 5), range(number % 5) FROM numbers(10);

SELECT * FROM t_parts_columns_filenames ORDER BY id;

SELECT name, column, type, serialization_kind, substreams, filenames
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_parts_columns_filenames'
ORDER BY name, column;

DROP TABLE IF EXISTS t_parts_columns_filenames;
