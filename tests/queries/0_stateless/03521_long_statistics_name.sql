DROP TABLE IF EXISTS t_long_statistics_name;

CREATE TABLE t_long_statistics_name (
`一个非常非常非常非常非常非常非常非常非常非常非常长的中文字符串` Int,
)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS replace_long_file_name_to_hash = 1, max_file_name_length = 127, auto_statistics_types = 'minmax,uniq';

INSERT INTO t_long_statistics_name VALUES (10) (100);

SELECT
    rows,
    statistics,
    estimates.cardinality,
    estimates.min,
    estimates.max,
FROM system.parts_columns
WHERE table = 't_long_statistics_name'
    AND database = currentDatabase()
    AND table = 't_long_statistics_name'
    AND column = '一个非常非常非常非常非常非常非常非常非常非常非常长的中文字符串';

DROP TABLE IF EXISTS t_long_statistics_name;
