-- https://github.com/ClickHouse/ClickHouse/issues/9587#issuecomment-944431385

CREATE TABLE source (a Int32) ENGINE=MergeTree() ORDER BY tuple();
CREATE TABLE source_null AS source ENGINE=Null;
CREATE TABLE dest_a (count UInt32, min Int32, max Int32, count_subquery Int32, min_subquery Int32, max_subquery Int32) ENGINE=MergeTree() ORDER BY tuple();

CREATE MATERIALIZED VIEW mv_null TO source_null AS SELECT * FROM source;
CREATE MATERIALIZED VIEW mv_a to dest_a AS
SELECT
    count() AS count,
    min(a) AS min,
    max(a) AS max,
    (SELECT count() FROM source_null) AS count_subquery,
    (SELECT min(a) FROM source_null) AS min_subquery,
    (SELECT max(a) FROM source_null) AS max_subquery
FROM source_null
GROUP BY count_subquery, min_subquery, max_subquery;

SET optimize_trivial_insert_select = 1;
INSERT INTO source SELECT number FROM numbers(2000) SETTINGS min_insert_block_size_rows=1500, max_insert_block_size=1500;

SELECT count() FROM source;
SELECT count() FROM dest_a;
SELECT * from dest_a ORDER BY count DESC;
