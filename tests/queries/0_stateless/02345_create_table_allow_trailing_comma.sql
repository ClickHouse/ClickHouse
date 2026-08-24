DROP TABLE IF EXISTS trailing_comma_1 SYNC;
CREATE TABLE trailing_comma_1 (id INT NOT NULL DEFAULT 1,) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE TABLE trailing_comma_1;
DROP TABLE trailing_comma_1;

DROP TABLE IF EXISTS trailing_comma_2 SYNC;
CREATE TABLE trailing_comma_2 (id INT DEFAULT 1,) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE TABLE trailing_comma_2;
DROP TABLE trailing_comma_2;

DROP TABLE IF EXISTS trailing_comma_3 SYNC;
CREATE TABLE trailing_comma_3 (x UInt8, y UInt8,) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE TABLE trailing_comma_3;
DROP TABLE trailing_comma_3;
