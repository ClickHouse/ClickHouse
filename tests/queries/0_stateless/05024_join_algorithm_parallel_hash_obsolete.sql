-- `join_algorithm = 'parallel_hash'` is accepted without a warning and reads back as `hash`.
-- It still runs a hash join under `compatibility = '26.8'`.
SET join_algorithm = 'parallel_hash';
SELECT value FROM system.settings WHERE name = 'join_algorithm';
SELECT count() FROM system.warnings WHERE message LIKE '%Obsolete setting%' AND message LIKE '%parallel_hash%';
SET join_algorithm = DEFAULT;
SELECT count() FROM numbers(10) AS t1 INNER JOIN numbers(10) AS t2 ON t1.number = t2.number
SETTINGS compatibility = '26.8', join_algorithm = 'parallel_hash';
