SELECT *
FROM (
  SELECT
    ([toString(number % 2)] :: Array(LowCardinality(String))) AS item_id,
    count()
  FROM numbers(3)
  GROUP BY item_id WITH TOTALS
) AS l FULL JOIN (
  SELECT
    ([toString((number % 2) * 2)] :: Array(String)) AS item_id
  FROM numbers(3)
) AS r
ON l.item_id = r.item_id
ORDER BY 1,2,3;
