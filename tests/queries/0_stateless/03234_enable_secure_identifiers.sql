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
    enforce_strict_identifier_format=true; -- { serverError BAD_ARGUMENTS }
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
    enforce_strict_identifier_format=true; -- { serverError BAD_ARGUMENTS }

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
    enforce_strict_identifier_format=true; -- { serverError BAD_ARGUMENTS }

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
    enforce_strict_identifier_format=true; -- { serverError BAD_ARGUMENTS }

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
    enforce_strict_identifier_format=true;

SHOW CREATE TABLE test_foo
SETTINGS
    enforce_strict_identifier_format=true;

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
    enforce_strict_identifier_format=true;

SHOW CREATE TABLE test_foo
SETTINGS
    enforce_strict_identifier_format=true;

-- CREATE TABLE without `enforce_strict_identifier_format`
DROP TABLE IF EXISTS test_foo;
CREATE TABLE `test_foo` (
    `insecure_$` Int8,
    `date` Date,
    `town` LowCardinality(String),
)
ENGINE = MergeTree
PRIMARY KEY (town, date)
PARTITION BY toYear(date);
-- Then SHOW CREATE .. with `enforce_strict_identifier_format`
-- While the result contains insecure identifiers (`insecure_$`), the `SHOW CREATE TABLE ...` query does not have any. So the query is expected to succeed.
SHOW CREATE TABLE test_foo
SETTINGS
    enforce_strict_identifier_format=true;

DROP TABLE IF EXISTS test_foo;

-- SHOW CREATE .. query contains an insecure identifier (`test_foo$`) with `enforce_strict_identifier_format`
SHOW CREATE TABLE `test_foo$`
SETTINGS
    enforce_strict_identifier_format=true; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS test_foo;
