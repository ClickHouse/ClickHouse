CREATE TABLE test_a (id UInt32, company UInt32, total UInt64) ENGINE=SummingMergeTree() PARTITION BY company PRIMARY KEY (id) ORDER BY (id, company);
INSERT INTO test_a SELECT number%10 as id, number%2 as company, count() as total FROM numbers(100) GROUP BY id,company;
CREATE TABLE test_b (id UInt32, company UInt32, total UInt64) ENGINE=SummingMergeTree() PARTITION BY company ORDER BY (id, company);
ALTER TABLE test_b REPLACE PARTITION '0' FROM test_a; -- {serverError BAD_ARGUMENTS}
