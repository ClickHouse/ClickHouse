-- Part of 00961_check_table test, but with in-memory parts
SET check_query_single_value_result = 0;
DROP TABLE IF EXISTS mt_table;
CREATE TABLE mt_table (d Date, key UInt64, data String) ENGINE = MergeTree() PARTITION BY toYYYYMM(d) ORDER BY key
    SETTINGS min_rows_for_compact_part = 1000, min_rows_for_compact_part = 1000;

CHECK TABLE mt_table;
INSERT INTO mt_table VALUES (toDate('2019-01-02'), 1, 'Hello'), (toDate('2019-01-02'), 2, 'World');
CHECK TABLE mt_table;
DROP TABLE mt_table;
