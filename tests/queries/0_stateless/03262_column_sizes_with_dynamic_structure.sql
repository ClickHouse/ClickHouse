-- Tags: no-random-settings, no-fasttest

set allow_experimental_dynamic_type = 1;
SET enable_json_type = 1;


drop table if exists test;
create table test (d Dynamic, json JSON) engine=MergeTree order by tuple() settings min_rows_for_wide_part=0, min_bytes_for_wide_part=1;
insert into test select number, '{"a" : 42, "b" : "Hello, World"}' from numbers(10000000);

SELECT
    `table`,
    sum(rows) AS rows,
    floor(sum(data_uncompressed_bytes) / (1024 * 1024)) AS data_size_uncompressed,
    floor(sum(data_compressed_bytes) / (1024 * 1024)) AS data_size_compressed,
    floor(sum(bytes_on_disk) / (1024 * 1024)) AS total_size_on_disk
FROM system.parts
WHERE active AND (database = currentDatabase()) AND (`table` = 'test')
GROUP BY `table`
ORDER BY `table` ASC;

drop table test;
