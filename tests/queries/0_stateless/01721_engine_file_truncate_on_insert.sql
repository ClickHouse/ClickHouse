-- Tags: no-replicated-database, no-parallel
-- Tag no-replicated-database: user_files

DROP TABLE IF EXISTS test;

INSERT INTO TABLE FUNCTION file('01721_file/test/data.TSV', 'TSV', 'id UInt32') VALUES (1);
ATTACH TABLE test FROM '01721_file/test' (id UInt8) ENGINE=File(TSV);

INSERT INTO test VALUES (2), (3);
INSERT INTO test VALUES (4);
SELECT * FROM test;

SET engine_file_truncate_on_insert=0;

INSERT INTO test VALUES (5), (6);
SELECT * FROM test;

SET engine_file_truncate_on_insert=1;

INSERT INTO test VALUES (0), (1), (2);
SELECT * FROM test;

SET engine_file_truncate_on_insert=0;
DROP TABLE test;
