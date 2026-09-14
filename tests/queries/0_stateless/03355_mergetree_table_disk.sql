-- Tags: no-parallel, no-fasttest
-- Tag no-parallel - uses external data source
-- Tag no-fasttest - requires SSL for https

DROP TABLE IF EXISTS uk_price_paid;

-- table_disk is supported only by s3_plain/s3_plain_rewritable/web
CREATE TABLE test_table_disk_requires_disk (key Int) ENGINE=MergeTree ORDER BY () SETTINGS table_disk=1; -- { serverError BAD_ARGUMENTS }
CREATE TABLE test_table_disk_requires_proper_disk (key Int) ENGINE=MergeTree ORDER BY () SETTINGS disk='default', table_disk=1; -- { serverError BAD_ARGUMENTS }

CREATE TABLE uk_price_paid
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    is_new UInt8,
    duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2)
SETTINGS disk = disk(type = web, endpoint = 'https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/store/cf7/cf712b4f-2ca8-435c-ac23-c4393efe52f7/'), table_disk=1;
SELECT count() FROM uk_price_paid;

ALTER TABLE uk_price_paid MODIFY SETTING table_disk = 0; -- { serverError TABLE_IS_READ_ONLY }

-- drop does not hung
DROP TABLE uk_price_paid;

-- now let's ensure that the table_disk is immutable
CREATE TABLE test_table_disk_is_immutable (key Int) ENGINE=MergeTree ORDER BY tuple();
ALTER TABLE test_table_disk_is_immutable MODIFY SETTING table_disk = 1; -- { serverError READONLY_SETTING }
