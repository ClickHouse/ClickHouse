-- https://github.com/ClickHouse/ClickHouse/issues/123270
-- FINAL and SAMPLE on the source table inside a subquery of a materialized view query are
-- validated against the real table engine, not against the pushed block.

DROP TABLE IF EXISTS mv_final;
DROP TABLE IF EXISTS mv_sample;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS src_sample;

CREATE TABLE src (k UInt32) ENGINE = ReplacingMergeTree ORDER BY k;
CREATE TABLE src_sample (k UInt32) ENGINE = MergeTree ORDER BY intHash32(k) SAMPLE BY intHash32(k);
CREATE TABLE dst (k UInt32, c UInt64) ENGINE = MergeTree ORDER BY k;

CREATE MATERIALIZED VIEW mv_final TO dst AS SELECT k, (SELECT count() FROM src FINAL) AS c FROM src;
CREATE MATERIALIZED VIEW mv_sample TO dst AS SELECT k, (SELECT count() FROM src_sample SAMPLE 1) AS c FROM src_sample;

INSERT INTO src VALUES (1), (2);
INSERT INTO src_sample VALUES (3);

SELECT * FROM dst ORDER BY k;

DROP TABLE mv_final;
DROP TABLE mv_sample;
DROP TABLE dst;
DROP TABLE src;
DROP TABLE src_sample;
