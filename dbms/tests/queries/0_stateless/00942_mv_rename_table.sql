DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS view_table;
DROP TABLE IF EXISTS new_view_table;

CREATE TABLE src (x UInt8) ENGINE = Null;

CREATE MATERIALIZED VIEW view_table Engine = Memory AS SELECT * FROM src;

INSERT INTO src VALUES (1), (2), (3);
SELECT * FROM view_table ORDER BY x;

--Check if we can rename the view and if we can still fetch datas

RENAME TABLE view_table TO new_view_table;
SELECT * FROM new_view_table ORDER BY x;

DROP TABLE src;
DROP TABLE IF EXISTS view_table;
DROP TABLE IF EXISTS new_view_table;
