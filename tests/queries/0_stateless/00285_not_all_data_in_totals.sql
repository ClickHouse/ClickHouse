SET output_format_write_statistics = 0;
SET group_by_two_level_threshold = 1;
SELECT ignore(x), count() FROM (SELECT number AS x FROM system.numbers LIMIT 1000 UNION ALL SELECT number AS x FROM system.numbers LIMIT 1000) GROUP BY x WITH TOTALS LIMIT 10 FORMAT JSONCompact;
SELECT ignore(x), count() FROM (SELECT number AS x FROM system.numbers LIMIT 1000 UNION ALL SELECT number AS x FROM system.numbers LIMIT 1000) GROUP BY x WITH TOTALS ORDER BY x LIMIT 10 FORMAT JSONCompact;
