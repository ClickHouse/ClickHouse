DROP TABLE IF EXISTS `test_foo_#`;
CREATE TABLE `test_foo_#` (
    `date` Date,
    `town` LowCardinality(String),
)
ENGINE = MergeTree
PRIMARY KEY (town, date)
PARTITION BY toYear(date)
COMMENT 'test' -- to end ENGINE definition, so SETTINGS will be in the query level
SETTINGS
    enable_secure_identifiers=true; -- { serverError BAD_ARGUMENTS }
DROP TABLE IF EXISTS `test_foo_#`;


DROP TABLE IF EXISTS test_foo;
CREATE TABLE test_foo (
    `insecure_#` Int8,
    `date` Date,
    `town` LowCardinality(String),
)
ENGINE = MergeTree
PRIMARY KEY (town, date)
PARTITION BY toYear(date)
COMMENT 'test' -- to end ENGINE definition, so SETTINGS will be in the query level
SETTINGS
    enable_secure_identifiers=true; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS test_foo;
CREATE TABLE test_foo (
    `insecure_'` Int8,
    `date` Date,
    `town` LowCardinality(String),
)
ENGINE = MergeTree
PRIMARY KEY (town, date)
PARTITION BY toYear(date)
COMMENT 'test' -- to end ENGINE definition, so SETTINGS will be in the query level
SETTINGS
    enable_secure_identifiers=true; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS test_foo;
CREATE TABLE test_foo (
    `insecure_"` Int8,
    `date` Date,
    `town` LowCardinality(String),
)
ENGINE = MergeTree
PRIMARY KEY (town, date)
PARTITION BY toYear(date)
COMMENT 'test' -- to end ENGINE definition, so SETTINGS will be in the query level
SETTINGS
    enable_secure_identifiers=true; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS test_foo;
CREATE TABLE test_foo (
    `secure_123` Int8,
    `date` Date,
    `town` LowCardinality(String),
)
ENGINE = MergeTree
PRIMARY KEY (town, date)
PARTITION BY toYear(date)
COMMENT 'test' -- to end ENGINE definition, so SETTINGS will be in the query level
SETTINGS
    enable_secure_identifiers=true;

SHOW CREATE TABLE test_foo 
SETTINGS
    enable_secure_identifiers=true;

DROP TABLE IF EXISTS test_foo;
CREATE TABLE test_foo (
    `123_secure` Int8,
    `date` Date,
    `town` LowCardinality(String),
)
ENGINE = MergeTree
PRIMARY KEY (town, date)
PARTITION BY toYear(date)
COMMENT 'test' -- to end ENGINE definition, so SETTINGS will be in the query level
SETTINGS
    enable_secure_identifiers=true;

SHOW CREATE TABLE test_foo 
SETTINGS
    enable_secure_identifiers=true;

-- CREATE TABLE without `enable_secure_identifiers`
DROP TABLE IF EXISTS test_foo;
CREATE TABLE `test_foo` (
    `insecure_$` Int8,
    `date` Date,
    `town` LowCardinality(String),
)
ENGINE = MergeTree
PRIMARY KEY (town, date)
PARTITION BY toYear(date);
-- Then SHOW CREATE .. with `enable_secure_identifiers`
-- While the result contains insecure identifiers (`insecure_$`), the `SHOW CREATE TABLE ...` query does not have any. So the query is expected to succeed.
SHOW CREATE TABLE test_foo 
SETTINGS
    enable_secure_identifiers=true;

DROP TABLE IF EXISTS test_foo;

-- SHOW CREATE .. query contains an insecure identifier (`test_foo$`) with `enable_secure_identifiers`
SHOW CREATE TABLE `test_foo$`
SETTINGS
    enable_secure_identifiers=true; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS test_foo;