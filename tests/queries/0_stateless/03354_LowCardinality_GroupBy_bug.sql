-- https://github.com/ClickHouse/ClickHouse/issues/75247
CREATE TABLE a (x LowCardinality(String)) ENGINE=Memory;
CREATE TABLE b (x LowCardinality(String), y Float64) ENGINE=Memory;

INSERT INTO a 
SELECT 
  (number % 500)::String AS x
FROM (
    SELECT number
    FROM numbers(40000000)
)
ORDER BY x;

INSERT INTO b
SELECT 
  (number % 500)::String x,
  0 as y
FROM (SELECT number FROM numbers(500));

select count() from (
  SELECT any(y)
  FROM a
  left join b using x
  group by x
  order by x
);
