CREATE TABLE test (id UInt32, val1 String, val2 String, val3 String, val4 String) ENGINE = MergeTree ORDER BY id;

INSERT INTO test(id, val1, val2, val3, val4) SELECT number as id, randomPrintableASCII(7) as val1, 'https://' as val2, '.com' as val3, concat(val2, val1, val3) FROM system.numbers limit 2500;

INSERT INTO test(id, val1, val2, val3, val4) SELECT number as id, randomPrintableASCII(7) as val1, 'https://' as val2, '.com' as val3, concat(val3, val2, val1) FROM system.numbers limit 2500;

DEDUCE TABLE test BY val4

