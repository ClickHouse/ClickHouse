-- https://github.com/ClickHouse/ClickHouse/issues/124647
-- The pushed block stands in for the source table also in a correlated subquery, `IN table` and a non-leftmost table of an `IN` subquery.

DROP TABLE IF EXISTS mv_correlated;
DROP TABLE IF EXISTS mv_not_exists;
DROP TABLE IF EXISTS mv_in_table;
DROP TABLE IF EXISTS mv_in_join;
DROP TABLE IF EXISTS mv_null_in_table;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS src_null;

CREATE TABLE src (k UInt32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE src_null (k UInt32) ENGINE = Null;
CREATE TABLE dst (name String, k UInt32, c_self UInt64, c_prev UInt64) ENGINE = MergeTree ORDER BY (name, k);

INSERT INTO src VALUES (1);

-- `ifNull`: a correlated `count()` over no rows returns NULL, https://github.com/ClickHouse/ClickHouse/issues/111615
CREATE MATERIALIZED VIEW mv_correlated TO dst AS
    SELECT 'correlated' AS name, s.k AS k,
        (SELECT count() FROM src AS t WHERE t.k = s.k) AS c_self,
        ifNull((SELECT count() FROM src AS t WHERE t.k = s.k - 1), 0) AS c_prev
    FROM src AS s;
CREATE MATERIALIZED VIEW mv_not_exists TO dst AS
    SELECT 'not_exists' AS name, s.k AS k, 1 AS c_self, 0 AS c_prev
    FROM src AS s WHERE NOT EXISTS (SELECT 1 FROM src AS t WHERE t.k = s.k - 1);
CREATE MATERIALIZED VIEW mv_in_table TO dst AS
    SELECT 'in_table' AS name, k, toUInt64(k IN src) AS c_self, toUInt64((k - 1) IN src) AS c_prev FROM src;
CREATE MATERIALIZED VIEW mv_in_join TO dst AS
    SELECT 'in_join' AS name, k,
        toUInt64(k IN (SELECT t.k FROM numbers(10) AS n INNER JOIN src AS t ON n.number = t.k)) AS c_self,
        toUInt64((k - 1) IN (SELECT t.k FROM numbers(10) AS n INNER JOIN src AS t ON n.number = t.k)) AS c_prev
    FROM src;
CREATE MATERIALIZED VIEW mv_null_in_table TO dst AS
    SELECT 'null_in_table' AS name, k, toUInt64(k IN src_null) AS c_self, toUInt64((k - 1) IN src_null) AS c_prev FROM src_null;

INSERT INTO src VALUES (2);
INSERT INTO src_null VALUES (2);

SELECT * FROM dst ORDER BY name, k;

DROP TABLE mv_correlated;
DROP TABLE mv_not_exists;
DROP TABLE mv_in_table;
DROP TABLE mv_in_join;
DROP TABLE mv_null_in_table;
DROP TABLE dst;
DROP TABLE src;
DROP TABLE src_null;
