SELECT number, 'Hello' AS k FROM (SELECT number FROM system.numbers LIMIT 1000000) ORDER BY number LIMIT 999990, 100 SETTINGS max_bytes_before_external_sort = 1000000;
SELECT number, 'Hello' AS k FROM (SELECT number FROM system.numbers LIMIT 1000000) ORDER BY number, k LIMIT 999990, 100 SETTINGS max_bytes_before_external_sort = 1000000;
SELECT number, 'Hello' AS k FROM (SELECT number FROM system.numbers LIMIT 1000000) ORDER BY k, number, k LIMIT 999990, 100 SETTINGS max_bytes_before_external_sort = 1000000;
SELECT number, 'Hello' AS k FROM (SELECT number FROM system.numbers LIMIT 1000000) ORDER BY number, k, number LIMIT 999990, 100 SETTINGS max_bytes_before_external_sort = 1000000;
