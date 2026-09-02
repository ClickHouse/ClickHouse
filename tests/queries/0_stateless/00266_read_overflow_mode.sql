SELECT number AS k FROM (SELECT number FROM system.numbers LIMIT 110000 SETTINGS max_result_rows = 0) GROUP BY k ORDER BY k LIMIT 10 SETTINGS max_result_rows = 100000, result_overflow_mode = 'break';
