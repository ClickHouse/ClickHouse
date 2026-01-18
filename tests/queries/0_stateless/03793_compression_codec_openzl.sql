-- Tags: no-fasttest

SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS test_openzl;

-- Test basic table creation with OpenZL codec
CREATE TABLE test_openzl
(
    id UInt64 CODEC(OpenZL),
    value Float64 CODEC(OpenZL),
    counter UInt32 CODEC(OpenZL),
    timestamp DateTime CODEC(OpenZL)
) ENGINE = MergeTree() ORDER BY id;

-- Insert test data
INSERT INTO test_openzl SELECT number, number * 1.5, number % 1000, now() + number FROM numbers(10000);

-- Verify row count
SELECT count() FROM test_openzl;

-- Verify data integrity
SELECT * FROM test_openzl ORDER BY id LIMIT 5;

-- Verify aggregations work
SELECT sum(id), avg(value), max(counter) FROM test_openzl;

-- Test OPTIMIZE (triggers compression)
OPTIMIZE TABLE test_openzl FINAL;

-- Verify after optimize
SELECT count() FROM test_openzl;

-- Test DETACH/ATTACH
DETACH TABLE test_openzl;
ATTACH TABLE test_openzl;

SELECT count() FROM test_openzl;

DROP TABLE test_openzl;

-- Test OpenZL with generic profile parameter
DROP TABLE IF EXISTS test_openzl_profile;

CREATE TABLE test_openzl_profile
(
    id UInt64 CODEC(OpenZL('generic'))
) ENGINE = MergeTree() ORDER BY id;

INSERT INTO test_openzl_profile SELECT number FROM numbers(1000);
SELECT count() FROM test_openzl_profile;

DROP TABLE test_openzl_profile;

-- Test that OpenZL requires experimental codecs setting
SET allow_experimental_codecs = 0;
CREATE TABLE test_openzl_fail(id UInt64 CODEC(OpenZL)) ENGINE = MergeTree() ORDER BY id; -- { serverError BAD_ARGUMENTS }
