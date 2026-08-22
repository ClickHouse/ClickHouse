SELECT '=== ataptive granularity: table one -; table two + ===';

DROP TABLE IF EXISTS table_one;
CREATE TABLE table_one (id UInt64, value UInt64)
ENGINE = MergeTree
PARTITION BY id
ORDER BY value
SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 100;

DROP TABLE IF EXISTS table_two;
CREATE TABLE table_two (id UInt64, value UInt64)
ENGINE = MergeTree
PARTITION BY id
ORDER BY value
SETTINGS index_granularity = 8192, index_granularity_bytes = 1024, min_bytes_for_wide_part = 100;

INSERT INTO table_one SELECT intDiv(number, 10), number   FROM numbers(100);

ALTER TABLE table_two REPLACE PARTITION 0 FROM table_one;

SELECT '=== ataptive granularity: table one -; table two - ===';

DROP TABLE IF EXISTS table_one;

CREATE TABLE table_one (id UInt64, value UInt64)
ENGINE = MergeTree
PARTITION BY id
ORDER BY value
SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 100;

DROP TABLE IF EXISTS table_two;

CREATE TABLE table_two (id UInt64, value UInt64)
ENGINE = MergeTree
PARTITION BY id
ORDER BY value
SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 100;

INSERT INTO table_one SELECT intDiv(number, 10), number   FROM numbers(100);

ALTER TABLE table_two REPLACE PARTITION 0 FROM table_one;

SELECT '=== ataptive granularity: table one +; table two + ===';

DROP TABLE IF EXISTS table_one;
CREATE TABLE table_one (id UInt64, value UInt64)
ENGINE = MergeTree
PARTITION BY id
ORDER BY value
SETTINGS index_granularity = 8192, index_granularity_bytes = 1024, min_bytes_for_wide_part = 100;

DROP TABLE IF EXISTS table_two;
CREATE TABLE table_two (id UInt64, value UInt64)
ENGINE = MergeTree
PARTITION BY id
ORDER BY value
SETTINGS index_granularity = 8192, index_granularity_bytes = 1024, min_bytes_for_wide_part = 100;

INSERT INTO table_one SELECT intDiv(number, 10), number   FROM numbers(100);

ALTER TABLE table_two REPLACE PARTITION 0 FROM table_one;

SELECT '=== ataptive granularity: table one +; table two - ===';

DROP TABLE IF EXISTS table_one;
CREATE TABLE table_one (id UInt64, value UInt64)
ENGINE = MergeTree
PARTITION BY id
ORDER BY value
SETTINGS index_granularity = 8192, index_granularity_bytes = 1024, min_bytes_for_wide_part = 100;

DROP TABLE IF EXISTS table_two;
CREATE TABLE table_two (id UInt64, value UInt64)
ENGINE = MergeTree
PARTITION BY id
ORDER BY value
SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 100;

INSERT INTO table_one SELECT intDiv(number, 10), number   FROM numbers(100);

ALTER TABLE table_two REPLACE PARTITION 0 FROM table_one; -- { serverError BAD_ARGUMENTS }
