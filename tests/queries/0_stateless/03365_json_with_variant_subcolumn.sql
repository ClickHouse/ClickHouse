SET allow_experimental_variant_type = 1;
SET enable_json_type = 1;

DROP TABLE IF EXISTS json_test;
CREATE TABLE json_test
(
    `id` String,
    `json` JSON(
        foo Variant(String, Array(String))
    )
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/json_test', '1')
PARTITION BY tuple()
ORDER BY id
SETTINGS index_granularity = 8192, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO json_test VALUES('1', '{"foo":"bar"}');

SELECT count(*) FROM json_test;

DROP TABLE json_test;
