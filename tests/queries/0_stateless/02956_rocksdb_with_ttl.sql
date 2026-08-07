-- Tags: no-ordinary-database, use-rocksdb

-- TTL = 2s
CREATE TABLE dict_with_ttl (key UInt64, value String) ENGINE = EmbeddedRocksDB(2) PRIMARY KEY (key);
INSERT INTO dict_with_ttl VALUES (0, 'foo');
-- Data inserted correctly
SELECT * FROM dict_with_ttl;
-- If possible, we should test that even we execute OPTIMIZE TABLE, the data is still there if TTL is not expired yet
-- Nevertheless, query time is unpredictable with different builds, so we can't test it. So we only test that after 3s
-- we execute OPTIMIZE and the data should be gone.
SELECT sleep(3);
OPTIMIZE TABLE dict_with_ttl;
SELECT * FROM dict_with_ttl;
