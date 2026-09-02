DROP TABLE IF EXISTS t_long_partition_column_name;

CREATE TABLE t_long_partition_column_name (
`一个非常非常非常非常非常非常非常非常非常非常非常长的中文字符串` Int,
)
ENGINE = MergeTree()
PARTITION BY `一个非常非常非常非常非常非常非常非常非常非常非常长的中文字符串`
ORDER BY tuple()
SETTINGS replace_long_file_name_to_hash = 1, max_file_name_length = 127;

insert into t_long_partition_column_name values(1);

SELECT * FROM t_long_partition_column_name;

DROP TABLE IF EXISTS t_long_partition_column_name;