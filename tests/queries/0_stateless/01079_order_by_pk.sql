DROP TABLE IF EXISTS mt_pk;

CREATE TABLE mt_pk ENGINE = MergeTree PARTITION BY d ORDER BY x SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi'
AS SELECT toDate(number % 32) AS d, number AS x FROM system.numbers LIMIT 1000010;
SELECT x FROM mt_pk ORDER BY x ASC LIMIT 1000000, 1;

DROP TABLE mt_pk;
