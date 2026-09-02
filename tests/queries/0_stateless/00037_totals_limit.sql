SET output_format_write_statistics = 0;

SELECT count(), arrayJoin([1, 2, 3]) AS n GROUP BY n WITH TOTALS ORDER BY n LIMIT 1 FORMAT JSON;
