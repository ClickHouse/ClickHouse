CREATE TABLE table_02184 (x UInt8); --{serverError 119}
SET default_table_engine = 'Log';
CREATE TABLE table_02184 (x UInt8);
SHOW CREATE TABLE table_02184;
DROP TABLE table_02184;
SET default_table_engine = 'MergeTree';
CREATE TABLE table_02184 (x UInt8); --{serverError 42}
CREATE TABLE table_02184 (x UInt8, PRIMARY KEY (x));
SHOW CREATE TABLE table_02184;
DROP TABLE table_02184;
CREATE TABLE table_02184 (x UInt8) PARTITION BY x; --{serverError 36}
CREATE TABLE table_02184 (x UInt8) ORDER BY x;
SHOW CREATE TABLE table_02184;
DROP TABLE table_02184;
CREATE TABLE table_02184 (x UInt8) PRIMARY KEY x;
SHOW CREATE TABLE table_02184;
DROP TABLE table_02184;
