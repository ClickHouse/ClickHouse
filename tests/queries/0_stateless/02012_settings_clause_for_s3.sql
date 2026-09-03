-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

DROP TABLE IF EXISTS table_with_range;

CREATE TABLE table_with_range(`name` String,`number` UInt32)　ENGINE = S3('http://localhost:11111/test/tsv_with_header.tsv', 'test', 'testtest', 'TSVWithNames')　SETTINGS input_format_with_names_use_header = 1;

select * from table_with_range;

DROP TABLE IF EXISTS table_with_range;
