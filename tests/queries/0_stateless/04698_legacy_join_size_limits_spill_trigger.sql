-- With `legacy_join_size_limits_trigger_spilling` the two size limits make a join spill again, the way they
-- did before. `compatibility` turns it on.

SELECT 'compatibility restores the legacy behavior';
SELECT value FROM system.settings WHERE name = 'legacy_join_size_limits_trigger_spilling' SETTINGS compatibility = '26.8';
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

-- Legacy mode ignores the spill threshold for standalone `grace_hash`, the way it did before it
-- applied there. A threshold of 1 byte would otherwise rehash every bucket until it runs out.
SELECT 'legacy: the spill threshold does not apply';
SET max_bytes_before_external_join = 1;
SET max_bytes_in_join = '4M';
SELECT count()
FROM (SELECT number AS k FROM numbers(2000000)) AS t1
INNER JOIN (SELECT number AS k FROM numbers(2000000)) AS t2
USING (k);
