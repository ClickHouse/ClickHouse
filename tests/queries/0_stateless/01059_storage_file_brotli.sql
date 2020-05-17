DROP TABLE IF EXISTS file;
CREATE TABLE file (x UInt64) ENGINE = File(TSV, 'data1.tsv.br');
TRUNCATE TABLE file;

INSERT INTO file SELECT * FROM numbers(1000000);
SELECT count(), max(x) FROM file;

DROP TABLE file;

CREATE TABLE file (x UInt64) ENGINE = File(TSV, 'data2.tsv.gz');
TRUNCATE TABLE file;

INSERT INTO file SELECT * FROM numbers(1000000);
SELECT count(), max(x) FROM file;

DROP TABLE file;

SELECT count(), max(x) FROM file('data{1,2}.tsv.{gz,br}', TSV, 'x UInt64');

-- check that they are compressed
SELECT count() < 1000000, max(x) FROM file('data1.tsv.br', RowBinary, 'x UInt8', 'none');
SELECT count() < 3000000, max(x) FROM file('data2.tsv.gz', RowBinary, 'x UInt8', 'none');
