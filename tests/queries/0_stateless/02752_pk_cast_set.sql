CREATE TABLE tst_in (key UInt32) ENGINE=MergeTree ORDER BY key AS SELECT number FROM numbers(1000000);

SELECT count() FROM tst_in WHERE key IN (SELECT number FROM numbers(1000)) SETTINGS force_primary_key = 1, max_rows_to_read = 9192; -- +1000 for sub query
-- not able to use primary key, but won't throw exception
SELECT count() FROM tst_in WHERE key IN (SELECT toString(number) FROM numbers(1000));
