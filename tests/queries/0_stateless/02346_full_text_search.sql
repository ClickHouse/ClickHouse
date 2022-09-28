SET log_queries = 1;
SET allow_experimental_inverted_index = 1;

-- create table for inverted(2)
DROP TABLE IF EXISTS simple1;
CREATE TABLE simple1(k UInt64,s String,INDEX af (s) TYPE inverted(2) GRANULARITY 1) 
            ENGINE = MergeTree() ORDER BY k
            SETTINGS index_granularity = 2;
-- insert test data into table
INSERT INTO simple1 VALUES (101, 'Alick a01'), (102, 'Blick a02'), (103, 'Click a03'),(104, 'Dlick a04'),(105, 'Elick a05'),(106, 'Alick a06'),(107, 'Blick a07'),(108, 'Click a08'),(109, 'Dlick a09'),(110, 'Elick b10'),(111, 'Alick b01'),(112, 'Blick b02'),(113, 'Click b03'),(114, 'Dlick b04'),(115, 'Elick b05'),(116, 'Alick b06'),(117, 'Blick b07'),(118, 'Click b08'),(119, 'Dlick b09'),(120, 'Elick b10');
-- check inverted index was created
SELECT name, type FROM system.data_skipping_indices where (table =='simple1') limit 1;

-- search inverted index
SELECT * FROM simple1 WHERE s LIKE '%01%' order by k;
SYSTEM FLUSH LOGS;
-- check the query only read 2 granules (4 rows total; each granule has 2 rows)
SELECT read_rows, result_rows from system.query_log 
        where query_kind ='Select'
            and current_database = currentDatabase()
            and endsWith(trimRight(query), 'SELECT * FROM simple1 WHERE s LIKE \'%01%\';') 
            and result_rows==2 limit 1;

-- create table for inverted()
DROP TABLE IF EXISTS simple2;
CREATE TABLE simple2(k UInt64,s String,INDEX af (s) TYPE inverted() GRANULARITY 1) 
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 2;

-- insert test data into table
INSERT INTO simple2 VALUES (101, 'Alick a01'), (102, 'Blick a02'), (103, 'Click a03'),(104, 'Dlick a04'),(105, 'Elick a05'),(106, 'Alick a06'),(107, 'Blick a07'),(108, 'Click a08'),(109, 'Dlick a09'),(110, 'Elick b10'),(111, 'Alick b01'),(112, 'Blick b02'),(113, 'Click b03'),(114, 'Dlick b04'),(115, 'Elick b05'),(116, 'Alick b06'),(117, 'Blick b07'),(118, 'Click b08'),(119, 'Dlick b09'),(120, 'Elick b10');

-- check inverted index was created
SELECT name, type FROM system.data_skipping_indices where (table =='simple2') limit 1;
-- search inverted index
SELECT * FROM simple2 WHERE hasToken(s, 'Alick') order by k;
SYSTEM FLUSH LOGS;
-- check the query only read 4 granules (8 rows total; each granule has 2 rows)
SELECT read_rows, result_rows from system.query_log 
    where query_kind ='Select'
        and current_database = currentDatabase()
        and endsWith(trimRight(query), 'SELECT * FROM simple2 WHERE hasToken(s, \'Alick\');') 
        and result_rows==4 limit 1;

-- create table for inverted(2) with two parts
DROP TABLE IF EXISTS simple3;
CREATE TABLE simple3(k UInt64,s String,INDEX af (s) TYPE inverted(2) GRANULARITY 1)
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 2;

-- insert test data into table
INSERT INTO simple3 VALUES (101, 'Alick a01'), (102, 'Blick a02'), (103, 'Click a03'),(104, 'Dlick a04'),(105, 'Elick a05'),(106, 'Alick a06'),(107, 'Blick a07'),(108, 'Click a08'),(109, 'Dlick a09'),(110, 'Elick b10'),(111, 'Alick b01'),(112, 'Blick b02'),(113, 'Click b03'),(114, 'Dlick b04'),(115, 'Elick b05'),(116, 'Alick b06'),(117, 'Blick b07'),(118, 'Click b08'),(119, 'Dlick b09'),(120, 'Elick b10');
INSERT INTO simple3 VALUES (201, 'rick c01'), (202, 'mick c02'),(203, 'nick c03');
-- check inverted index was created
SELECT name, type FROM system.data_skipping_indices where (table =='simple3') limit 1;
-- search inverted index
SELECT * FROM simple3 WHERE s LIKE '%01%' order by k;
SYSTEM FLUSH LOGS;
-- check the query only read 3 granules (6 rows total; each granule has 2 rows)
SELECT read_rows, result_rows from system.query_log 
    where query_kind ='Select' 
        and current_database = currentDatabase()
        and endsWith(trimRight(query), 'SELECT * FROM simple3 WHERE s LIKE \'%01%\';') 
        and result_rows==3 limit 1;

-- create table for inverted(2) for utf8 string test
DROP TABLE IF EXISTS simple4;
CREATE TABLE simple4(k UInt64,s String,INDEX af (s) TYPE inverted(2) GRANULARITY 1) ENGINE = MergeTree() ORDER BY k
SETTINGS index_granularity = 2;
-- insert test data into table
INSERT INTO simple4 VALUES (101, 'Alick 好'),(102, 'clickhouse你好'), (103, 'Click 你'),(104, 'Dlick 你a好'),(105, 'Elick 好好你你'),(106, 'Alick 好a好a你a你');
-- check inverted index was created
SELECT name, type FROM system.data_skipping_indices where (table =='simple4') limit 1;
-- search inverted index
SELECT * FROM simple4 WHERE s LIKE '%你好%' order by k;
SYSTEM FLUSH LOGS;
-- check the query only read 1 granule (2 rows total; each granule has 2 rows)
SELECT read_rows, result_rows from system.query_log 
    where query_kind ='Select' 
        and current_database = currentDatabase()    
        and endsWith(trimRight(query), 'SELECT * FROM simple4 WHERE s LIKE \'%你好%\';') 
        and result_rows==1 limit 1;

-- create table for max_digestion_size_per_segment test
DROP TABLE IF EXISTS simple5;
CREATE TABLE simple5(k UInt64,s String,INDEX af(s) TYPE inverted(0) GRANULARITY 1)
                     Engine=MergeTree
                          ORDER BY (k)
                          SETTINGS max_digestion_size_per_segment = 1024, index_granularity = 256
                          AS
                          SELECT
                          number,
                          format('{},{},{},{}', hex(12345678), hex(87654321), hex(number/17 + 5), hex(13579012)) as s
                          FROM numbers(10240);

-- check inverted index was created
SELECT name, type FROM system.data_skipping_indices where (table =='simple5') limit 1;
-- search inverted index
SELECT s FROM simple5 WHERE hasToken(s, '6969696969898240');
SYSTEM FLUSH LOGS;
-- check the query only read 1 granule (1 row total; each granule has 256 rows)
SELECT read_rows, result_rows from system.query_log 
        where query_kind ='Select'
            and current_database = currentDatabase()
            and startsWith(query, 'SELECT s FROM simple5 WHERE hasToken') 
            and result_rows==1 limit 1;
