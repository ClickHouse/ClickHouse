--Tags: no-fasttest

CREATE TABLE table_fsst_codec (n String CODEC(FSST)) ENGINE = Memory;
INSERT INTO table_fsst_codec VALUES ('Hello'), ('world'), ('!');
SELECT * FROM table_fsst_codec;
