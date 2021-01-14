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

CREATE TABLE file (x UInt64) ENGINE = File(TSV, 'data3.tsv.xz');
TRUNCATE TABLE file;

INSERT INTO file SELECT * FROM numbers(1000000);
SELECT count(), max(x) FROM file;

DROP TABLE file;

CREATE TABLE file (x UInt64) ENGINE = File(TSV, 'data4.tsv.zst');
TRUNCATE TABLE file;

INSERT INTO file SELECT * FROM numbers(1000000);
SELECT count(), max(x) FROM file;

DROP TABLE file;

SELECT count(), max(x) FROM file('data{1,2,3,4}.tsv.{gz,br,xz,zst}', TSV, 'x UInt64');

-- check that they are compressed
SELECT count() < 1000000, max(x) FROM file('data1.tsv.br', RowBinary, 'x UInt8', 'none');
SELECT count() < 3000000, max(x) FROM file('data2.tsv.gz', RowBinary, 'x UInt8', 'none');
SELECT count() < 1000000, max(x) FROM file('data3.tsv.xz', RowBinary, 'x UInt8', 'none');
SELECT count() < 1000000, max(x) FROM file('data4.tsv.zst', RowBinary, 'x UInt8', 'none');
