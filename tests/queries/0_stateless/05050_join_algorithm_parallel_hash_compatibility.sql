-- `join_algorithm` dropped `parallel_hash` from the default list in 26.9.
-- `compatibility = '26.8'` must restore `direct,parallel_hash,hash,ie_join`.
-- An explicit `parallel_hash` still names HashJoin; the unification is not
-- gated on compatibility.

SELECT 'compat_26_8_default_list';
SELECT getSetting('join_algorithm') SETTINGS compatibility = '26.8';

SELECT 'explicit_parallel_hash';
SELECT getSetting('join_algorithm')
SETTINGS compatibility = '26.8', join_algorithm = 'parallel_hash';

SELECT 'explicit_parallel_hash_join';
SELECT count()
FROM numbers(10) AS t1 INNER JOIN numbers(10) AS t2 ON t1.number = t2.number
SETTINGS compatibility = '26.8', join_algorithm = 'parallel_hash';
