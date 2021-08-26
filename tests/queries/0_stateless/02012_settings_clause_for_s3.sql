DROP TABLE IF EXISTS table_with_range;

CREATE TABLE table_with_range
(
    `name` String,
    `value` UInt32
)
ENGINE = S3('https://storage.yandexcloud.net/my-test-bucket-768/{some,another}_prefix/some_file_{1..3}', 'CSV')
SETTINGS input_format_with_names_use_header = 0;

DROP TABLE IF EXISTS table_with_range;
