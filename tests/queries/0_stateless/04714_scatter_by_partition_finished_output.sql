-- The query below only covers the bug while the plan really scatters, so pin the scatter first.
SELECT 'scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 2
FROM (EXPLAIN PIPELINE
  SELECT count()
  FROM (SELECT number AS k FROM numbers(4000) ORDER BY k ASC) AS l
  INNER JOIN (SELECT number % NULL AS k FROM numbers(100) ORDER BY k) AS r ON l.k = r.k
  SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, max_block_size = 16);

SELECT count()
FROM (SELECT number AS k FROM numbers(4000) ORDER BY k ASC) AS l
INNER JOIN (SELECT number % NULL AS k FROM numbers(100) ORDER BY k) AS r ON l.k = r.k
SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, max_block_size = 16;
