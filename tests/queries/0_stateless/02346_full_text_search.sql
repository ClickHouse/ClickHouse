SET log_queries = 1;
SET allow_experimental_inverted_index = 1;

-- create table for inverted(2)
DROP TABLE IF EXISTS simple1;
CREATE TABLE simple1(k UInt64,s String,INDEX af (s) TYPE inverted(2) GRANULARITY 1) 
            ENGINE = MergeTree() ORDER BY k
            SETTINGS index_granularity = 2;
-- insert test data into table
INSERT INTO simple1 VALUES (101, 'Alick a01'), (102, 'Blick a02'), (103, 'Click a03'),(104, 'Dlick a04'),(105, 'Elick a05'),(106, 'Alick a06'),(107, 'Blick a07'),(108, 'Click a08'),(109, 'Dlick a09'),(110, 'Elick a10'),(111, 'Alick b01'),(112, 'Blick b02'),(113, 'Click b03'),(114, 'Dlick b04'),(115, 'Elick b05'),(116, 'Alick b06'),(117, 'Blick b07'),(118, 'Click b08'),(119, 'Dlick b09'),(120, 'Elick b10');
-- check inverted index was created
SELECT name, type FROM system.data_skipping_indices where (table =='simple1') limit 1;

-- search inverted index with ==
SELECT * FROM simple1 WHERE s == 'Alick a01';
SYSTEM FLUSH LOGS;
-- check the query only read 1 granules (2 rows total; each granule has 2 rows)
SELECT read_rows==2 from system.query_log 
        where query_kind ='Select'
            and current_database = currentDatabase()
            and endsWith(trimRight(query), 'SELECT * FROM simple1 WHERE s == \'Alick a01\';')
            and type='QueryFinish'
            and result_rows==1
            limit 1;

-- search inverted index with LIKE
SELECT * FROM simple1 WHERE s LIKE '%01%' ORDER BY k;
SYSTEM FLUSH LOGS;
-- check the query only read 2 granules (4 rows total; each granule has 2 rows)
SELECT read_rows==4 from system.query_log 
        where query_kind ='Select'
            and current_database = currentDatabase()
            and endsWith(trimRight(query), 'SELECT * FROM simple1 WHERE s LIKE \'%01%\' ORDER BY k;')
            and type='QueryFinish'
            and result_rows==2
            limit 1;

-- search inverted index with hasToken
SELECT * FROM simple1 WHERE hasToken(s, 'Click') ORDER BY k;
SYSTEM FLUSH LOGS;
-- check the query only read 4 granules (8 rows total; each granule has 2 rows)
SELECT read_rows==8 from system.query_log 
        where query_kind ='Select'
            and current_database = currentDatabase()
            and endsWith(trimRight(query), 'SELECT * FROM simple1 WHERE hasToken(s, \'Click\') ORDER BY k;')
            and type='QueryFinish' 
            and result_rows==4 limit 1;

-- create table for inverted()
DROP TABLE IF EXISTS simple2;
CREATE TABLE simple2(k UInt64,s String,INDEX af (s) TYPE inverted() GRANULARITY 1) 
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 2;

-- insert test data into table
INSERT INTO simple2 VALUES (101, 'Alick a01'), (102, 'Blick a02'), (103, 'Click a03'),(104, 'Dlick a04'),(105, 'Elick a05'),(106, 'Alick a06'),(107, 'Blick a07'),(108, 'Click a08'),(109, 'Dlick a09'),(110, 'Elick a10'),(111, 'Alick b01'),(112, 'Blick b02'),(113, 'Click b03'),(114, 'Dlick b04'),(115, 'Elick b05'),(116, 'Alick b06'),(117, 'Blick b07'),(118, 'Click b08'),(119, 'Dlick b09'),(120, 'Elick b10');

-- check inverted index was created
SELECT name, type FROM system.data_skipping_indices where (table =='simple2') limit 1;

-- search inverted index with hasToken
SELECT * FROM simple2 WHERE hasToken(s, 'Alick') order by k;
SYSTEM FLUSH LOGS;
-- check the query only read 4 granules (8 rows total; each granule has 2 rows)
SELECT read_rows==8 from system.query_log 
    where query_kind ='Select'
        and current_database = currentDatabase()
        and endsWith(trimRight(query), 'SELECT * FROM simple2 WHERE hasToken(s, \'Alick\');') 
        and type='QueryFinish' 
        and result_rows==4 limit 1;

-- search inverted index with IN operator
SELECT * FROM simple2 WHERE s IN ('Alick a01', 'Alick a06') ORDER BY k;
SYSTEM FLUSH LOGS;
-- check the query only read 2 granules (4 rows total; each granule has 2 rows)
SELECT read_rows==4 from system.query_log 
    where query_kind ='Select'
        and current_database = currentDatabase()
        and endsWith(trimRight(query), 'SELECT * FROM simple2 WHERE s IN (\'Alick a01\', \'Alick a06\') ORDER BY k;') 
        and type='QueryFinish' 
        and result_rows==2 limit 1;

-- search inverted index with multiSearch        
SELECT * FROM simple2 WHERE multiSearchAny(s, ['a01', 'b01']) ORDER BY k;
SYSTEM FLUSH LOGS;
-- check the query only read 2 granules (4 rows total; each granule has 2 rows)
SELECT read_rows==4 from system.query_log 
    where query_kind ='Select'
        and current_database = currentDatabase()
        and endsWith(trimRight(query), 'SELECT * FROM simple2 WHERE multiSearchAny(s, [\'a01\', \'b01\']) ORDER BY k;') 
        and type='QueryFinish' 
        and result_rows==2 limit 1;

-- create table with an array column
DROP TABLE IF EXISTS simple_array;
create table simple_array (k UInt64, s Array(String), INDEX af (s) TYPE inverted(2) GRANULARITY 1)
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 2;
INSERT INTO simple_array SELECT rowNumberInBlock(), groupArray(s) FROM simple2 GROUP BY k%10;
-- check inverted index was created
SELECT name, type FROM system.data_skipping_indices where (table =='simple_array') limit 1;
-- search inverted index with has
SELECT * FROM simple_array WHERE has(s, 'Click a03') ORDER BY k;
SYSTEM FLUSH LOGS;
-- check the query must read all 10 granules (20 rows total; each granule has 2 rows)
SELECT read_rows==2 from system.query_log 
    where query_kind ='Select'
        and current_database = currentDatabase()
        and endsWith(trimRight(query), 'SELECT * FROM simple_array WHERE has(s, \'Click a03\') ORDER BY k;') 
        and type='QueryFinish' 
        and result_rows==1 limit 1;

-- create table with a map column
DROP TABLE IF EXISTS simple_map;
CREATE TABLE simple_map (k UInt64, s Map(String,String), INDEX af (mapKeys(s)) TYPE inverted(2) GRANULARITY 1)
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 2;
INSERT INTO simple_map VALUES (101, {'Alick':'Alick a01'}), (102, {'Blick':'Blick a02'}), (103, {'Click':'Click a03'}),(104, {'Dlick':'Dlick a04'}),(105, {'Elick':'Elick a05'}),(106, {'Alick':'Alick a06'}),(107, {'Blick':'Blick a07'}),(108, {'Click':'Click a08'}),(109, {'Dlick':'Dlick a09'}),(110, {'Elick':'Elick a10'}),(111, {'Alick':'Alick b01'}),(112, {'Blick':'Blick b02'}),(113, {'Click':'Click b03'}),(114, {'Dlick':'Dlick b04'}),(115, {'Elick':'Elick b05'}),(116, {'Alick':'Alick b06'}),(117, {'Blick':'Blick b07'}),(118, {'Click':'Click b08'}),(119, {'Dlick':'Dlick b09'}),(120, {'Elick':'Elick b10'});
-- check inverted index was created
SELECT name, type FROM system.data_skipping_indices where (table =='simple_map') limit 1;
-- search inverted index with mapContains
SELECT * FROM simple_map WHERE mapContains(s, 'Click') ORDER BY k;
SYSTEM FLUSH LOGS;
-- check the query must read all 4 granules (8 rows total; each granule has 2 rows)
SELECT read_rows==8 from system.query_log 
    where query_kind ='Select'
        and current_database = currentDatabase()
        and endsWith(trimRight(query), 'SELECT * FROM simple_map WHERE mapContains(s, \'Click\') ORDER BY k;') 
        and type='QueryFinish' 
        and result_rows==4 limit 1;

-- search inverted index with map key
SELECT * FROM simple_map WHERE s['Click'] = 'Click a03';
SYSTEM FLUSH LOGS;
-- check the query must read all 4 granules (8 rows total; each granule has 2 rows)
SELECT read_rows==8 from system.query_log 
    where query_kind ='Select'
        and current_database = currentDatabase()
        and endsWith(trimRight(query), 'SELECT * FROM simple_map WHERE s[\'Click\'] = \'Click a03\';') 
        and type='QueryFinish' 
        and result_rows==1 limit 1;

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
SELECT read_rows==6 from system.query_log
    where query_kind ='Select' 
        and current_database = currentDatabase()
        and endsWith(trimRight(query), 'SELECT * FROM simple3 WHERE s LIKE \'%01%\' order by k;')
        and type='QueryFinish' 
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
SELECT read_rows==2 from system.query_log 
    where query_kind ='Select' 
        and current_database = currentDatabase()    
        and endsWith(trimRight(query), 'SELECT * FROM simple4 WHERE s LIKE \'%你好%\' order by k;') 
        and type='QueryFinish' 
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
SELECT read_rows==256 from system.query_log 
        where query_kind ='Select'
            and current_database = currentDatabase()
            and endsWith(trimRight(query), 'SELECT s FROM simple5 WHERE hasToken(s, \'6969696969898240\');') 
            and type='QueryFinish' 
            and result_rows==1 limit 1;

DROP TABLE IF EXISTS simple6;
-- create inverted index with density==1
CREATE TABLE simple6(k UInt64,s String,INDEX af(s) TYPE inverted(0, 1.0) GRANULARITY 1)
                     Engine=MergeTree
                          ORDER BY (k)
                          SETTINGS max_digestion_size_per_segment = 1, index_granularity = 512
                          AS
                          SELECT number, if(number%2, format('happy {}', hex(number)), format('birthday {}', hex(number)))
                          FROM numbers(1024);
-- check inverted index was created
SELECT name, type FROM system.data_skipping_indices where (table =='simple6') limit 1;
-- search inverted index, no row has 'happy birthday'
SELECT count()==0 FROM simple6 WHERE s=='happy birthday';
SYSTEM FLUSH LOGS;
-- check the query only skip all granules (0 row total; each granule has 512 rows)
SELECT read_rows==0 from system.query_log 
        where query_kind ='Select'
            and current_database = currentDatabase()
            and endsWith(trimRight(query), 'SELECT count()==0 FROM simple6 WHERE s==\'happy birthday\';')
            and type='QueryFinish' 
            and result_rows==1 limit 1;

DROP TABLE IF EXISTS simple7;
-- create inverted index with density==0.1
CREATE TABLE simple7(k UInt64,s String,INDEX af(s) TYPE inverted(0, 0.1) GRANULARITY 1)
                    Engine=MergeTree
                        ORDER BY (k)
                        SETTINGS max_digestion_size_per_segment = 1, index_granularity = 512
                        AS
                        SELECT number, if(number==1023, 'happy new year', if(number%2, format('happy {}', hex(number)), format('birthday {}', hex(number))))
                        FROM numbers(1024);
-- check inverted index was created
SELECT name, type FROM system.data_skipping_indices where (table =='simple7') limit 1;
-- search inverted index, no row has 'happy birthday'
SELECT count()==0 FROM simple7 WHERE s=='happy birthday';
SYSTEM FLUSH LOGS;
-- check the query does not skip any of the 2 granules(1024 rows total; each granule has 512 rows)
SELECT read_rows==1024 from system.query_log 
        where query_kind ='Select'
            and current_database = currentDatabase()
            and endsWith(trimRight(query), 'SELECT count()==0 FROM simple7 WHERE s==\'happy birthday\';')
            and type='QueryFinish' 
            and result_rows==1 limit 1;
-- search inverted index, no row has 'happy new year'
SELECT count()==1 FROM simple7 WHERE s=='happy new year';
SYSTEM FLUSH LOGS;
-- check the query only read 1 granule because of density (1024 rows total; each granule has 512 rows)
SELECT read_rows==512 from system.query_log 
        where query_kind ='Select'
            and current_database = currentDatabase()
            and endsWith(trimRight(query), 'SELECT count()==1 FROM simple7 WHERE s==\'happy new year\';')
            and type='QueryFinish' 
            and result_rows==1 limit 1;

