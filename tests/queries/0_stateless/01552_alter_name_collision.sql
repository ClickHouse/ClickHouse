DROP TABLE IF EXISTS test;
CREATE TABLE test(test String DEFAULT 'test', test_tmp Int DEFAULT 1)ENGINE = Memory;
DROP TABLE test;
