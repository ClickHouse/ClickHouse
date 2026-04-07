-- Fixes issue: https://github.com/ClickHouse/ClickHouse/issues/84668

SELECT count() FROM (SELECT number AS c0 FROM numbers(10000)) tx JOIN (SELECT number AS c0 FROM numbers(10000)) ty ON tx.c0 = ty.c0 SETTINGS max_bytes_in_join = 1024, join_on_disk_max_files_to_merge = 18446744073709551615, join_algorithm = 'prefer_partial_merge';
