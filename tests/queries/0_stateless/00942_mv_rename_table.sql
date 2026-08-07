DROP TABLE IF EXISTS src_00942;
DROP TABLE IF EXISTS view_table_00942;
DROP TABLE IF EXISTS new_view_table_00942;

CREATE TABLE src_00942 (x UInt8) ENGINE = Null;

CREATE MATERIALIZED VIEW view_table_00942 Engine = Memory AS SELECT * FROM src_00942;

INSERT INTO src_00942 VALUES (1), (2), (3);
SELECT * FROM view_table_00942 ORDER BY x;

--Check if we can rename the view and if we can still fetch datas

RENAME TABLE view_table_00942 TO new_view_table_00942;
SELECT * FROM new_view_table_00942 ORDER BY x;

DROP TABLE src_00942;
DROP TABLE IF EXISTS view_table_00942;
DROP TABLE IF EXISTS new_view_table_00942;
