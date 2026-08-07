SET optimize_aggregators_of_group_by_keys = 1;
SELECT source.key, max(target.key) FROM (SELECT 1 key, 'x' name) source
INNER JOIN (SELECT 2 key, 'x' name) target
ON source.name = target.name
GROUP BY source.key;
