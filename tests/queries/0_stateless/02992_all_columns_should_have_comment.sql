SYSTEM FLUSH LOGS;
SELECT 'Column ' || name || ' from table ' || concat(database, '.', table) || ' should have a comment'
FROM system.columns
WHERE (database = 'system') AND
      (comment = '') AND
      (table NOT ILIKE '%log%') AND
      (table NOT IN ('numbers', 'numbers_mt', 'one', 'generate_series', 'generateSeries', 'coverage_log', 'filesystem_read_prefetches_log')) AND
      (default_kind != 'ALIAS');
