SELECT id, value
FROM
  (SELECT materialize(toLowCardinality(toNullable(0))) AS id, 1 AS value) AS a
  JOIN (SELECT toLowCardinality(0) AS id) AS d
USING (id)
SETTINGS enable_join_runtime_filters=0;


SELECT id, value
FROM
  (SELECT materialize(toLowCardinality(toNullable(0))) AS id, 1 AS value) AS a
  JOIN (SELECT toLowCardinality(0) AS id) AS d
USING (id)
SETTINGS enable_join_runtime_filters=1;

