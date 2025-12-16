-- Tags: no-fasttest
-- no-fasttest: It can be slow

SET allow_experimental_full_text_index = 1;
SET log_queries = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0;

-- Affects the number of read rows.
SET use_skip_indexes_on_data_read = 0;

----------------------------------------------------
SELECT 'Test text(tokenizer = ngram(2))';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(k UInt64, s String, INDEX af(s) TYPE text(tokenizer = ngrams(2)) GRANULARITY 1)
            ENGINE = MergeTree() ORDER BY k
            SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO tab VALUES (101, 'Alick a01'), (102, 'Blick a02'), (103, 'Click a03'), (104, 'Dlick a04'), (105, 'Elick a05'), (106, 'Alick a06'), (107, 'Blick a07'), (108, 'Click a08'), (109, 'Dlick a09'), (110, 'Elick a10'), (111, 'Alick b01'), (112, 'Blick b02'), (113, 'Click b03'), (114, 'Dlick b04'), (115, 'Elick b05'), (116, 'Alick b06'), (117, 'Blick b07'), (118, 'Click b08'), (119, 'Dlick b09'), (120, 'Elick b10');

-- check text index was created
SELECT name, type FROM system.data_skipping_indices WHERE table =='tab' AND database = currentDatabase() LIMIT 1;

-- throw in a random consistency check
CHECK TABLE tab SETTINGS check_query_single_value_result = 1;

-- search text index with ==
SELECT * FROM tab WHERE s == 'Alick a01';

-- check the query only read 1 granules (2 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS query_log;
SELECT read_rows==2 from system.query_log
        WHERE query_kind ='Select'
            AND current_database = currentDatabase()
            AND endsWith(trimRight(query), 'SELECT * FROM tab WHERE s == \'Alick a01\';')
            AND type='QueryFinish'
            AND result_rows==1
        LIMIT 1;

-- search text index with LIKE
SELECT * FROM tab WHERE s LIKE '%01%' ORDER BY k;

-- check the query only read 2 granules (4 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS query_log;
SELECT read_rows==4 from system.query_log
        WHERE query_kind ='Select'
            AND current_database = currentDatabase()
            AND endsWith(trimRight(query), 'SELECT * FROM tab WHERE s LIKE \'%01%\' ORDER BY k;')
            AND type='QueryFinish'
            AND result_rows==2
        LIMIT 1;

-- search text index with hasToken
SELECT * FROM tab WHERE hasToken(s, 'Click') ORDER BY k;

-- check the query only read 4 granules (8 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS query_log;
SELECT read_rows==8 from system.query_log
        WHERE query_kind ='Select'
            AND current_database = currentDatabase()
            AND endsWith(trimRight(query), 'SELECT * FROM tab WHERE hasToken(s, \'Click\') ORDER BY k;')
            AND type='QueryFinish'
            AND result_rows==4
        LIMIT 1;

----------------------------------------------------
SELECT 'Test text(tokenizer = "splitByNonAlpha")';

DROP TABLE IF EXISTS tab_x;

CREATE TABLE tab_x(k UInt64, s String, INDEX af(s) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO tab_x VALUES (101, 'x Alick a01 y'), (102, 'x Blick a02 y'), (103, 'x Click a03 y'), (104, 'x Dlick a04 y'), (105, 'x Elick a05 y'), (106, 'x Alick a06 y'), (107, 'x Blick a07 y'), (108, 'x Click a08 y'), (109, 'x Dlick a09 y'), (110, 'x Elick a10 y'), (111, 'x Alick b01 y'), (112, 'x Blick b02 y'), (113, 'x Click b03 y'), (114, 'x Dlick b04 y'), (115, 'x Elick b05 y'), (116, 'x Alick b06 y'), (117, 'x Blick b07 y'), (118, 'x Click b08 y'), (119, 'x Dlick b09 y'), (120, 'x Elick b10 y');

-- check text index was created
SELECT name, type FROM system.data_skipping_indices WHERE table == 'tab_x' AND database = currentDatabase() LIMIT 1;

-- search text index with hasToken
SELECT * FROM tab_x WHERE hasToken(s, 'Alick') ORDER BY k;

-- check the query only read 4 granules (8 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS query_log;
SELECT read_rows==8 from system.query_log
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND endsWith(trimRight(query), 'SELECT * FROM tab_x WHERE hasToken(s, \'Alick\');')
        AND type='QueryFinish'
        AND result_rows==4
    LIMIT 1;

-- search text index with IN operator
SELECT * FROM tab_x WHERE s IN ('x Alick a01 y', 'x Alick a06 y') ORDER BY k;

-- check the query only read 2 granules (4 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS query_log;
SELECT read_rows==4 from system.query_log
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND endsWith(trimRight(query), 'SELECT * FROM tab_x WHERE s IN (\'x Alick a01 y\', \'x Alick a06 y\') ORDER BY k;')
        AND type='QueryFinish'
        AND result_rows==2
    LIMIT 1;

----------------------------------------------------
SELECT 'Test text(tokenizer = ngrams(2)) on a column with two parts';


DROP TABLE IF EXISTS tab;

CREATE TABLE tab(k UInt64, s String)
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO tab VALUES (101, 'Alick a01'), (102, 'Blick a02'), (103, 'Click a03'), (104, 'Dlick a04'), (105, 'Elick a05'), (106, 'Alick a06'), (107, 'Blick a07'), (108, 'Click a08'), (109, 'Dlick a09'), (110, 'Elick b10'), (111, 'Alick b01'), (112, 'Blick b02'), (113, 'Click b03'), (114, 'Dlick b04'), (115, 'Elick b05'), (116, 'Alick b06'), (117, 'Blick b07'), (118, 'Click b08'), (119, 'Dlick b09'), (120, 'Elick b10');
INSERT INTO tab VALUES (201, 'rick c01'), (202, 'mick c02'), (203, 'nick c03');

ALTER TABLE tab ADD INDEX af(s) TYPE text(tokenizer = ngrams(2)) GRANULARITY 1 SETTINGS mutations_sync = 2;
OPTIMIZE TABLE tab FINAL;

-- check text index was created
SELECT name, type FROM system.data_skipping_indices WHERE table == 'tab' AND database = currentDatabase() LIMIT 1;

-- search text index
SELECT * FROM tab WHERE s LIKE '%01%' ORDER BY k;

-- check the query only read 3 granules (6 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS query_log;
SELECT read_rows==6 from system.query_log
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND endsWith(trimRight(query), 'SELECT * FROM tab WHERE s LIKE \'%01%\' ORDER BY k;')
        AND type='QueryFinish'
        AND result_rows==3
    LIMIT 1;

----------------------------------------------------
SELECT 'Test text(tokenizer = ngrams(2)) on UTF-8 data';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(k UInt64, s String, INDEX af(s) TYPE text(tokenizer = ngrams(2)) GRANULARITY 1)
    ENGINE = MergeTree()
    ORDER BY k
    SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO tab VALUES (101, 'Alick 好'), (102, 'clickhouse你好'), (103, 'Click 你'), (104, 'Dlick 你a好'), (105, 'Elick 好好你你'), (106, 'Alick 好a好a你a你');

-- check text index was created
SELECT name, type FROM system.data_skipping_indices WHERE table == 'tab' AND database = currentDatabase() LIMIT 1;

-- search text index
SELECT * FROM tab WHERE s LIKE '%你好%' ORDER BY k;

-- check the query only read 1 granule (2 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS query_log;
SELECT read_rows==2 from system.query_log
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND endsWith(trimRight(query), 'SELECT * FROM tab WHERE s LIKE \'%你好%\' ORDER BY k;')
        AND type='QueryFinish'
        AND result_rows==1
    LIMIT 1;

----------------------------------------------------
SELECT 'Test text(tokenizer = sparseGrams(3, 100)) on UTF-8 data';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(k UInt64, s String, INDEX af(s) TYPE text(tokenizer = sparseGrams(3, 100)) GRANULARITY 1)
    ENGINE = MergeTree()
    ORDER BY k
    SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO tab VALUES (101, 'Alick 好'), (102, 'clickhouse你好'), (103, 'Click 你'), (104, 'Dlick 你a好'), (105, 'Elick 好好你你'), (106, 'Alick 好a好a你a你');

-- check text index was created
SELECT name, type FROM system.data_skipping_indices WHERE table == 'tab' AND database = currentDatabase() LIMIT 1;

-- search text index
SELECT * FROM tab WHERE s LIKE '%house你好%' ORDER BY k;

-- check the query only read 1 granule (2 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS query_log;
SELECT read_rows==2 from system.query_log
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND endsWith(trimRight(query), 'SELECT * FROM tab WHERE s LIKE \'%你好%\' ORDER BY k;')
        AND type='QueryFinish'
        AND result_rows==1
    LIMIT 1;

----------------------------------------------------
SELECT 'Test text(tokenizer = sparseGrams(3, 100, 4)) on UTF-8 data';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(k UInt64, s String, INDEX af(s) TYPE text(tokenizer = sparseGrams(3, 100, 4)) GRANULARITY 1)
    ENGINE = MergeTree()
    ORDER BY k
    SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO tab VALUES (101, 'Alick 好'), (102, 'clickhouse你好'), (103, 'Click 你'), (104, 'Dlick 你a好'), (105, 'Elick 好好你你'), (106, 'Alick 好a好a你a你');

-- check text index was created
SELECT name, type FROM system.data_skipping_indices WHERE table == 'tab' AND database = currentDatabase() LIMIT 1;

-- search text index
SELECT * FROM tab WHERE s LIKE '%house你好%' ORDER BY k;

-- check the query only read 1 granule (2 rows total; each granule has 2 rows)
SYSTEM FLUSH LOGS query_log;
SELECT read_rows==2 from system.query_log
    WHERE query_kind ='Select'
        AND current_database = currentDatabase()
        AND endsWith(trimRight(query), 'SELECT * FROM tab WHERE s LIKE \'%你好%\' ORDER BY k;')
        AND type='QueryFinish'
        AND result_rows==1
    LIMIT 1;
