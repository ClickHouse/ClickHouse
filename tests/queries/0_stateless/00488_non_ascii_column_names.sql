DROP TABLE IF EXISTS non_ascii;

SET allow_table_engine_tinylog=1;

CREATE TABLE non_ascii (`привет` String, `мир` String) ENGINE = TinyLog;
INSERT INTO non_ascii VALUES ('hello', 'world');
SELECT `привет` FROM non_ascii;
SELECT * FROM non_ascii;
DROP TABLE non_ascii;
