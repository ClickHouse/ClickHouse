DROP TABLE IF EXISTS tmp_01375;
DROP TABLE IF EXISTS table_tsv_01375;

CREATE TABLE tmp_01375 (n UInt32, s String) ENGINE = Memory;
CREATE TABLE table_tsv_01375 AS tmp_01375 ENGINE = File(TSVWithNames);

INSERT INTO table_tsv_01375 SELECT number as n, toString(n) as s FROM numbers(10);
INSERT INTO table_tsv_01375 SELECT number as n, toString(n) as s FROM numbers(10);
INSERT INTO table_tsv_01375 SELECT number as n, toString(n) as s FROM numbers(10);

SELECT * FROM table_tsv_01375;

DROP TABLE IF EXISTS tmp_01375;
DROP TABLE IF EXISTS table_tsv_01375;
