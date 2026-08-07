SET allow_suspicious_codecs = 1;

DROP TABLE IF EXISTS segfault_table;

CREATE TABLE segfault_table (id UInt16 CODEC(Delta(2))) ENGINE MergeTree() order by tuple();

INSERT INTO segfault_table VALUES (1111), (2222);

SELECT * FROM segfault_table;

DROP TABLE IF EXISTS segfault_table;
