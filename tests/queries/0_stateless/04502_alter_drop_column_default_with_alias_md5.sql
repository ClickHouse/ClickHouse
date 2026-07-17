-- Tags: no-openssl-fips
-- MD5 (nested-alias) case split from 04502_alter_drop_column_default_with_alias so FIPS still runs the generic case.

DROP TABLE IF EXISTS t_04502_nested;

CREATE TABLE t_04502_nested
(
    id UInt64,
    url String,
    md5 String MATERIALIZED lower(hex(MD5(url))),
    s3_url String DEFAULT concat('prefix/', substring(arrayStringConcat(arrayMap(i -> substring(lower(hex(MD5(url))) AS hx, i, 1), range(1, 4))) AS h, 1, 2), '/', h)
)
ENGINE = MergeTree ORDER BY id;

ALTER TABLE t_04502_nested DROP COLUMN md5;

INSERT INTO t_04502_nested (id, url) VALUES (1, 'example');
SELECT id, s3_url FROM t_04502_nested ORDER BY id;

DROP TABLE t_04502_nested;
