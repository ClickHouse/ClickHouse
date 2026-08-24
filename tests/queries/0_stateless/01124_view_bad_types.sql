DROP TABLE IF EXISTS source_table;
CREATE TABLE source_table (x UInt16) ENGINE = TinyLog;
INSERT INTO source_table SELECT * FROM system.numbers LIMIT 10;

DROP TABLE IF EXISTS dest_view;
CREATE VIEW dest_view (x UInt64) AS SELECT * FROM source_table;

SELECT x, any(x) FROM dest_view GROUP BY x ORDER BY x;

DROP TABLE dest_view;
DROP TABLE source_table;
