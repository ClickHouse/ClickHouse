INSERT INTO TABLE FUNCTION file('01718_file/test/data.TSV', 'TSV', 'id UInt32') VALUES ('file', 42);
ATTACH TABLE test FROM '01718_file/test' (id UInt8) ENGINE=File(TSV);

CREATE TABLE file_engine_table (id UInt32) ENGINE=File(TabSeparated);

INSERT INTO file_engine_table VALUES (1), (2), (3);
INSERT INTO file_engine_table VALUES (4);
SELECT * FROM file_engine_table;

SET engine_file_truncate_on_insert=0;

INSERT INTO file_engine_table VALUES (5), (6);
SELECT * FROM file_engine_table;

SET engine_file_truncate_on_insert=1;

INSERT INTO file_engine_table VALUES (0), (1), (2);
SELECT * FROM file_engine_table;

SET engine_file_truncate_on_insert=0;
