-- Tags: no-random-settings

set allow_experimental_dynamic_type = 1;
set allow_experimental_json_type = 1;


drop table if exists test;
create table test (d Dynamic, json JSON) engine=MergeTree order by tuple() settings min_rows_for_wide_part=0, min_bytes_for_wide_part=1;
insert into test select number, '{"a" : 42, "b" : "Hello, World"}' from numbers(10000000);

SELECT
    `table`,
    formatReadableQuantity(sum(rows)) AS rows,
    formatReadableSize(sum(data_uncompressed_bytes)) AS data_size_uncompressed,
    formatReadableSize(sum(data_compressed_bytes)) AS data_size_compressed,
    formatReadableSize(sum(bytes_on_disk)) AS total_size_on_disk
FROM system.parts
WHERE active AND (database = currentDatabase()) AND (`table` = 'test')
GROUP BY `table`
ORDER BY `table` ASC;

drop table test;

