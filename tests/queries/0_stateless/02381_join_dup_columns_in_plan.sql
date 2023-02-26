SET join_algorithm = 'hash';

EXPLAIN actions=0, description=0, header=1
SELECT * FROM ( SELECT 'key2' AS key ) AS s1
JOIN ( SELECT 'key1' AS key, '1' AS value UNION ALL SELECT 'key2' AS key, '1' AS value ) AS s2
USING (key);

SET join_algorithm = 'full_sorting_merge';

EXPLAIN actions=0, description=0, header=1
SELECT * FROM ( SELECT 'key2' AS key ) AS s1
JOIN ( SELECT 'key1' AS key, '1' AS value UNION ALL SELECT 'key2' AS key, '1' AS value ) AS s2
USING (key);
