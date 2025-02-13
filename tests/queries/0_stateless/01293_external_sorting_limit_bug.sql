SELECT number FROM (SELECT number FROM system.numbers LIMIT 999990) ORDER BY number ASC LIMIT 100, 65535 SETTINGS max_bytes_before_external_sort = 1000000 format Null
