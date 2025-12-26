-- Fixes issue: https://github.com/ClickHouse/ClickHouse/issues/84668

SELECT 1 FROM (SELECT 1 c0) tx JOIN (SELECT 1 c0) ty ON tx.c0 = ty.c0 SETTINGS default_max_bytes_in_join = 1, join_on_disk_max_files_to_merge = 18446744073709551615, join_algorithm = 'prefer_partial_merge';
