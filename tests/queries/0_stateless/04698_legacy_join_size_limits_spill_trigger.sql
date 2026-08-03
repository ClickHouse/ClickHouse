-- `legacy_join_size_limits_trigger_spilling` restores the pre-unification meaning of
-- `max_rows_in_join` / `max_bytes_in_join` on the spilling path, and is what `compatibility` turns on.

SELECT 'compatibility restores the legacy behavior';
SELECT value FROM system.settings WHERE name = 'legacy_join_size_limits_trigger_spilling' SETTINGS compatibility = '26.7';
SELECT value FROM system.settings WHERE name = 'legacy_join_size_limits_trigger_spilling';

SET legacy_join_size_limits_trigger_spilling = 1;
SET max_bytes_ratio_before_external_join = 0;
SET max_bytes_before_external_join = 0;
SET grace_hash_join_initial_buckets = 1;
SET join_algorithm = 'grace_hash';

SELECT 'legacy: max_bytes_in_join spills instead of failing';
SET max_bytes_in_join = '4M';
SELECT count()
FROM (SELECT number AS k FROM numbers(2000000)) AS t1
INNER JOIN (SELECT number AS k FROM numbers(2000000)) AS t2
USING (k);

SELECT 'legacy: max_rows_in_join spills instead of failing';
SET max_bytes_in_join = 0;
SET max_rows_in_join = 100000;
SELECT count()
FROM (SELECT number AS k FROM numbers(2000000)) AS t1
INNER JOIN (SELECT number AS k FROM numbers(2000000)) AS t2
USING (k);

SELECT 'legacy: grace_hash needs no spill threshold';
SET max_rows_in_join = 0;
SELECT count()
FROM (SELECT number AS k FROM numbers(10)) AS t1
INNER JOIN (SELECT number AS k FROM numbers(10)) AS t2
USING (k);
