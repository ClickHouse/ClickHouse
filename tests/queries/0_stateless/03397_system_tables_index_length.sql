-- https://github.com/ClickHouse/ClickHouse/issues/57590

DROP TABLE IF EXISTS index_length_test;
CREATE TABLE index_length_test (a UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO index_length_test SELECT number % 2 ? NULL : number FROM numbers(10);

-- information_schema.tables.index_length
-- Check that index_length is larger than 0 for MergeTree tables
SELECT if(index_length > 0, 'OK', 'FAIL')
FROM information_schema.tables
WHERE table_name = 'index_length_test'
    AND table_schema = currentDatabase();

DROP TABLE index_length_test;

-- information_schema.tables.index_length
-- Check that index_length is 0 for non-MergeTree tables
SELECT if(index_length = 0, 'OK', 'FAIL')
FROM information_schema.tables
WHERE table_name = 'tables'
    AND table_schema = 'system';
