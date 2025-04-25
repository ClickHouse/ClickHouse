SET enable_hypothesis_deduction = 1;

CREATE TABLE test1(id UInt32, val1 String, val2 String, val3 String, val4 String) ENGINE = MergeTree ORDER BY id;

INSERT INTO test1(id, val1, val2, val3, val4) SELECT number as id, randomPrintableASCII(7) as val1, 'https://' as val2, '.com' as val3, concat(val2, val1, val3) FROM system.numbers limit 1000;
INSERT INTO test1(id, val1, val2, val3, val4) SELECT number + 1000 as id, randomPrintableASCII(7) as val1, 'https://' as val2, '.com' as val3, concat(val2, val1, val3) FROM system.numbers limit 1000;
DEDUCE TABLE test1 BY val4;
OPTIMIZE TABLE test1;
DEDUCE TABLE test1 BY val4;

CREATE TABLE test2(id UInt32, val1 String, val2 String, val3 String, val4 String, val5 String, val6 String, val7 String) ENGINE = MergeTree ORDER BY id;
INSERT INTO test2(id, val1, val2, val3, val4, val5, val6, val7) SELECT number as id, concat(val3, val4) as val1, concat(val5, val6) as val2, randomPrintableASCII(3) as val3, randomPrintableASCII(4) as val4, randomPrintableASCII(5) as val5, randomPrintableASCII(6) as val6, concat(val1, val2) as val7 FROM system.numbers limit 1000;
INSERT INTO test2(id, val1, val2, val3, val4, val5, val6, val7) SELECT number as id, randomPrintableASCII(1) as val1, randomPrintableASCII(2) as val2, randomPrintableASCII(3) as val3, randomPrintableASCII(4) as val4, randomPrintableASCII(5) as val5, randomPrintableASCII(6) as val6, concat(val1, val2) as val7 FROM system.numbers limit 1000;
DEDUCE TABLE test2 BY val7;
OPTIMIZE TABLE test2;
DEDUCE TABLE test2 BY val7;
