SET allow_experimental_inverted_index = 1;
SET log_queries = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0;

----------------------------------------------------
SELECT 'Test inverted(2)';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(k UInt64, s String, INDEX af(s) TYPE inverted(2))
            ENGINE = MergeTree() ORDER BY k
            SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO tab VALUES (101, 'Alick a01'), (102, 'Blick a02'), (103, 'Click a03'), (104, 'Dlick a04'), (105, 'Elick a05'), (106, 'Alick a06'), (107, 'Blick a07'), (108, 'Click a08'), (109, 'Dlick a09'), (110, 'Elick a10'), (111, 'Alick b01'), (112, 'Blick b02'), (113, 'Click b03'), (114, 'Dlick b04'), (115, 'Elick b05'), (116, 'Alick b06'), (117, 'Blick b07'), (118, 'Click b08'), (119, 'Dlick b09'), (120, 'Elick b10');

-- check inverted index was created
SELECT name, type FROM system.data_skipping_indices WHERE table =='tab' AND database = currentDatabase() LIMIT 1;

-- throw in a random consistency check
CHECK TABLE tab;

-- search inverted index with ==
SELECT * FROM tab WHERE s == 'Alick a01';

-- check the query only read 1 granules (2 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS;
SELECT read_rows==2 from system.query_log
        WHERE query_kind ='Select'
            AND current_database = currentDatabase()
            AND endsWith(trimRight(query), 'SELECT * FROM tab WHERE s == \'Alick a01\';')
            AND type='QueryFinish'
            AND result_rows==1
        LIMIT 1;

-- search inverted index with LIKE
SELECT * FROM tab WHERE s LIKE '%01%' ORDER BY k;

-- check the query only read 2 granules (4 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS;
SELECT read_rows==4 from system.query_log
        WHERE query_kind ='Select'
            AND current_database = currentDatabase()
            AND endsWith(trimRight(query), 'SELECT * FROM tab WHERE s LIKE \'%01%\' ORDER BY k;')
            AND type='QueryFinish'
            AND result_rows==2
        LIMIT 1;

-- search inverted index with hasToken
SELECT * FROM tab WHERE hasToken(s, 'Click') ORDER BY k;

-- check the query only read 4 granules (8 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS;
SELECT read_rows==8 from system.query_log
        WHERE query_kind ='Select'
            AND current_database = currentDatabase()
            AND endsWith(trimRight(query), 'SELECT * FROM tab WHERE hasToken(s, \'Click\') ORDER BY k;')
            AND type='QueryFinish'
            AND result_rows==4
        LIMIT 1;

----------------------------------------------------
SELECT 'Test inverted()';

DROP TABLE IF EXISTS tab_x;

CREATE TABLE tab_x(k UInt64, s String, INDEX af(s) TYPE inverted())
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO tab_x VALUES (101, 'Alick a01'), (102, 'Blick a02'), (103, 'Click a03'), (104, 'Dlick a04'), (105, 'Elick a05'), (106, 'Alick a06'), (107, 'Blick a07'), (108, 'Click a08'), (109, 'Dlick a09'), (110, 'Elick a10'), (111, 'Alick b01'), (112, 'Blick b02'), (113, 'Click b03'), (114, 'Dlick b04'), (115, 'Elick b05'), (116, 'Alick b06'), (117, 'Blick b07'), (118, 'Click b08'), (119, 'Dlick b09'), (120, 'Elick b10');

-- check inverted index was created
SELECT name, type FROM system.data_skipping_indices WHERE table == 'tab_x' AND database = currentDatabase() LIMIT 1;

-- search inverted index with hasToken
SELECT * FROM tab_x WHERE hasToken(s, 'Alick') ORDER BY k;

-- check the query only read 4 granules (8 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS;
SELECT read_rows==8 from system.query_log
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND endsWith(trimRight(query), 'SELECT * FROM tab_x WHERE hasToken(s, \'Alick\');')
        AND type='QueryFinish'
        AND result_rows==4
    LIMIT 1;

-- search inverted index with IN operator
SELECT * FROM tab_x WHERE s IN ('Alick a01', 'Alick a06') ORDER BY k;

-- check the query only read 2 granules (4 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS;
SELECT read_rows==4 from system.query_log
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND endsWith(trimRight(query), 'SELECT * FROM tab_x WHERE s IN (\'Alick a01\', \'Alick a06\') ORDER BY k;')
        AND type='QueryFinish'
        AND result_rows==2
    LIMIT 1;

-- search inverted index with multiSearch
SELECT * FROM tab_x WHERE multiSearchAny(s, ['a01', 'b01']) ORDER BY k;

-- check the query only read 2 granules (4 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS;
SELECT read_rows==4 from system.query_log
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND endsWith(trimRight(query), 'SELECT * FROM tab_x WHERE multiSearchAny(s, [\'a01\', \'b01\']) ORDER BY k;')
        AND type='QueryFinish'
        AND result_rows==2
    LIMIT 1;

----------------------------------------------------
SELECT 'Test on array columns';

DROP TABLE IF EXISTS tab;

create table tab (k UInt64, s Array(String), INDEX af(s) TYPE inverted(2))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO tab SELECT rowNumberInBlock(), groupArray(s) FROM tab_x GROUP BY k%10;

-- check inverted index was created
SELECT name, type FROM system.data_skipping_indices WHERE table == 'tab' AND database = currentDatabase() LIMIT 1;

-- search inverted index with has
SELECT * FROM tab WHERE has(s, 'Click a03') ORDER BY k;

-- check the query must read all 10 granules (20 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS;
SELECT read_rows==2 from system.query_log
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND endsWith(trimRight(query), 'SELECT * FROM tab WHERE has(s, \'Click a03\') ORDER BY k;')
        AND type='QueryFinish'
        AND result_rows==1
    LIMIT 1;

----------------------------------------------------
SELECT 'Test on map columns';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (k UInt64, s Map(String,String), INDEX af(mapKeys(s)) TYPE inverted(2))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO tab VALUES (101, {'Alick':'Alick a01'}), (102, {'Blick':'Blick a02'}), (103, {'Click':'Click a03'}), (104, {'Dlick':'Dlick a04'}), (105, {'Elick':'Elick a05'}), (106, {'Alick':'Alick a06'}), (107, {'Blick':'Blick a07'}), (108, {'Click':'Click a08'}), (109, {'Dlick':'Dlick a09'}), (110, {'Elick':'Elick a10'}), (111, {'Alick':'Alick b01'}), (112, {'Blick':'Blick b02'}), (113, {'Click':'Click b03'}), (114, {'Dlick':'Dlick b04'}), (115, {'Elick':'Elick b05'}), (116, {'Alick':'Alick b06'}), (117, {'Blick':'Blick b07'}), (118, {'Click':'Click b08'}), (119, {'Dlick':'Dlick b09'}), (120, {'Elick':'Elick b10'});

-- check inverted index was created
SELECT name, type FROM system.data_skipping_indices WHERE table == 'tab' AND database = currentDatabase() LIMIT 1;

-- search inverted index with mapContains
SELECT * FROM tab WHERE mapContains(s, 'Click') ORDER BY k;

-- check the query must read all 4 granules (8 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS;
SELECT read_rows==8 from system.query_log
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND endsWith(trimRight(query), 'SELECT * FROM tab WHERE mapContains(s, \'Click\') ORDER BY k;')
        AND type='QueryFinish'
        AND result_rows==4
    LIMIT 1;

-- search inverted index with map key
SELECT * FROM tab WHERE s['Click'] = 'Click a03';

-- check the query must read all 4 granules (8 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS;
SELECT read_rows==8 from system.query_log
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND endsWith(trimRight(query), 'SELECT * FROM tab WHERE s[\'Click\'] = \'Click a03\';')
        AND type='QueryFinish'
        AND result_rows==1
    LIMIT 1;

----------------------------------------------------
SELECT 'Test inverted(2) on a column with two parts';


DROP TABLE IF EXISTS tab;

CREATE TABLE tab(k UInt64, s String, INDEX af(s) TYPE inverted(2))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO tab VALUES (101, 'Alick a01'), (102, 'Blick a02'), (103, 'Click a03'), (104, 'Dlick a04'), (105, 'Elick a05'), (106, 'Alick a06'), (107, 'Blick a07'), (108, 'Click a08'), (109, 'Dlick a09'), (110, 'Elick b10'), (111, 'Alick b01'), (112, 'Blick b02'), (113, 'Click b03'), (114, 'Dlick b04'), (115, 'Elick b05'), (116, 'Alick b06'), (117, 'Blick b07'), (118, 'Click b08'), (119, 'Dlick b09'), (120, 'Elick b10');
INSERT INTO tab VALUES (201, 'rick c01'), (202, 'mick c02'), (203, 'nick c03');

-- check inverted index was created
SELECT name, type FROM system.data_skipping_indices WHERE table == 'tab' AND database = currentDatabase() LIMIT 1;

-- search inverted index
SELECT * FROM tab WHERE s LIKE '%01%' ORDER BY k;

-- check the query only read 3 granules (6 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS;
SELECT read_rows==6 from system.query_log
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND endsWith(trimRight(query), 'SELECT * FROM tab WHERE s LIKE \'%01%\' ORDER BY k;')
        AND type='QueryFinish'
        AND result_rows==3
    LIMIT 1;

----------------------------------------------------
SELECT 'Test inverted(2) on UTF-8 data';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(k UInt64, s String, INDEX af(s) TYPE inverted(2))
    ENGINE = MergeTree()
    ORDER BY k
    SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO tab VALUES (101, 'Alick 好'), (102, 'clickhouse你好'), (103, 'Click 你'), (104, 'Dlick 你a好'), (105, 'Elick 好好你你'), (106, 'Alick 好a好a你a你');

-- check inverted index was created
SELECT name, type FROM system.data_skipping_indices WHERE table == 'tab' AND database = currentDatabase() LIMIT 1;

-- search inverted index
SELECT * FROM tab WHERE s LIKE '%你好%' ORDER BY k;

-- check the query only read 1 granule (2 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS;
SELECT read_rows==2 from system.query_log
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND endsWith(trimRight(query), 'SELECT * FROM tab WHERE s LIKE \'%你好%\' ORDER BY k;')
        AND type='QueryFinish'
        AND result_rows==1
    LIMIT 1;


----------------------------------------------------
SELECT 'AST Fuzzer crash, issue #54541';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (row_id UInt32, str String, INDEX idx str TYPE inverted) ENGINE = MergeTree ORDER BY row_id;
INSERT INTO tab VALUES (0, 'a');
SELECT * FROM tab WHERE str == 'b' AND 1.0;

SELECT 'Test max_rows_per_postings_list';
DROP TABLE IF EXISTS tab;
-- create table 'tab' with inverted index parameter (ngrams, max_rows_per_most_list) which is (0, 10240)
CREATE TABLE tab(k UInt64, s String, INDEX af(s) TYPE inverted(0, 12040))
                     Engine=MergeTree
                     ORDER BY (k)
                     AS
                         SELECT
                         number,
                         format('{},{},{},{}', hex(12345678), hex(87654321), hex(number/17 + 5), hex(13579012)) as s
                         FROM numbers(1024);
SELECT count(s) FROM tab WHERE hasToken(s, '4C4B4B4B4B4B5040');
DROP TABLE IF EXISTS tab;
-- create table 'tab' with inverted index parameter (ngrams, max_rows_per_most_list) which is (0, 123)
-- it should throw exception since max_rows_per_most_list(123) is less than its minimum value(8196)
CREATE TABLE tab(k UInt64, s String, INDEX af(s) TYPE inverted(3, 123))
                     Engine=MergeTree
                     ORDER BY (k)
                     AS
                         SELECT
                         number,
                         format('{},{},{},{}', hex(12345678), hex(87654321), hex(number/17 + 5), hex(13579012)) as s
                         FROM numbers(1024);  -- { serverError 80 }
