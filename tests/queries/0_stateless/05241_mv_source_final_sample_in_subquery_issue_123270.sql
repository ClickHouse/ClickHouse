-- https://github.com/ClickHouse/ClickHouse/issues/123270
-- FINAL and SAMPLE on the source table inside a subquery, JOIN or IN of a materialized view query
-- are validated against the real table engine, not against the pushed block.

DROP TABLE IF EXISTS mv_final;
DROP TABLE IF EXISTS mv_sample;
DROP TABLE IF EXISTS mv_join_final;
DROP TABLE IF EXISTS mv_join_table_final;
DROP TABLE IF EXISTS mv_in_final;
DROP TABLE IF EXISTS mv_join_sample;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS src_sample;

CREATE TABLE src (k UInt32) ENGINE = ReplacingMergeTree ORDER BY k;
CREATE TABLE src_sample (k UInt32) ENGINE = MergeTree ORDER BY intHash32(k) SAMPLE BY intHash32(k);
CREATE TABLE dst (name String, k UInt32, c UInt64) ENGINE = MergeTree ORDER BY (name, k);

CREATE MATERIALIZED VIEW mv_final TO dst AS
    SELECT 'scalar_final' AS name, k, (SELECT count() FROM src FINAL) AS c FROM src;
CREATE MATERIALIZED VIEW mv_sample TO dst AS
    SELECT 'scalar_sample' AS name, k, (SELECT count() FROM src_sample SAMPLE 1) AS c FROM src_sample;
CREATE MATERIALIZED VIEW mv_join_final TO dst AS
    SELECT 'join_final' AS name, s.k AS k, t.c AS c FROM src AS s CROSS JOIN (SELECT count() AS c FROM src FINAL) AS t;
CREATE MATERIALIZED VIEW mv_join_table_final TO dst AS
    SELECT 'join_table_final' AS name, s.k AS k, count() AS c FROM src AS s INNER JOIN src AS t FINAL ON s.k = t.k GROUP BY s.k;
CREATE MATERIALIZED VIEW mv_in_final TO dst AS
    SELECT 'in_final' AS name, k, toUInt64(k IN (SELECT k FROM src FINAL)) AS c FROM src;
CREATE MATERIALIZED VIEW mv_join_sample TO dst AS
    SELECT 'join_sample' AS name, s.k AS k, t.c AS c FROM src_sample AS s CROSS JOIN (SELECT count() AS c FROM src_sample SAMPLE 1) AS t;

INSERT INTO src VALUES (1), (2);
INSERT INTO src_sample VALUES (3);

SELECT * FROM dst ORDER BY name, k;

DROP TABLE mv_final;
DROP TABLE mv_sample;
DROP TABLE mv_join_final;
DROP TABLE mv_join_table_final;
DROP TABLE mv_in_final;
DROP TABLE mv_join_sample;
DROP TABLE dst;
DROP TABLE src;
DROP TABLE src_sample;
