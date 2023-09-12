CREATE TABLE tst_in (key UInt32) ENGINE=MergeTree ORDER BY key AS SELECT number FROM numbers(1000000);
-- analyzer hasn't supported building set with sub-query yet
SELECT count() FROM tst_in WHERE key IN (SELECT number FROM numbers(1000)) SETTINGS force_primary_key = 1;
-- not able to use primary key, but won't throw exception
SELECT count() FROM tst_in WHERE key IN (SELECT toString(number) FROM numbers(1000));
