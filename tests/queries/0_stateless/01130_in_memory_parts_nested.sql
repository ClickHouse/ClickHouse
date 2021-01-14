-- Test 00576_nested_and_prewhere, but with in-memory parts.
DROP TABLE IF EXISTS nested;

CREATE TABLE nested (x UInt64, filter UInt8, n Nested(a UInt64)) ENGINE = MergeTree ORDER BY x
    SETTINGS min_rows_for_compact_part = 200000, min_rows_for_wide_part = 300000;

INSERT INTO nested SELECT number, number % 2, range(number % 10) FROM system.numbers LIMIT 100000;

ALTER TABLE nested ADD COLUMN n.b Array(UInt64);
SELECT DISTINCT n.b FROM nested PREWHERE filter;
SELECT DISTINCT n.b FROM nested PREWHERE filter SETTINGS max_block_size = 10;
SELECT DISTINCT n.b FROM nested PREWHERE filter SETTINGS max_block_size = 123;

ALTER TABLE nested ADD COLUMN n.c Array(UInt64) DEFAULT arrayMap(x -> x * 2, n.a);
SELECT DISTINCT n.c FROM nested PREWHERE filter;
SELECT DISTINCT n.a, n.c FROM nested PREWHERE filter;

DROP TABLE nested;
