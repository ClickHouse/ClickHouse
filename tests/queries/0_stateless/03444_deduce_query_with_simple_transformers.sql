SET enable_hypothesis_deduction = 1;

CREATE TABLE test1(id UInt32, val1 String, val2 String, val3 String, val4 String) ENGINE = MergeTree ORDER BY id;

INSERT INTO test1(id, val1, val2, val3, val4) SELECT number as id, randomPrintableASCII(2) as val1, randomPrintableASCII(3) as val2, randomPrintableASCII(4) as val3, concatWithSeparator(',', val2, val1, val3) FROM system.numbers limit 2500;
INSERT INTO test1(id, val1, val2, val3, val4) SELECT number as id, 'some_constant_string' as val1, randomPrintableASCII(3) as val2, randomPrintableASCII(4) as val3, concatWithSeparator(val1, val2, val3) FROM system.numbers limit 2500;

DEDUCE TABLE test1 BY val4;

CREATE TABLE test2(id UInt32, base String, derived String) ENGINE = MergeTree ORDER BY id;
INSERT INTO test2(id, base, derived) SELECT number as id, randomPrintableASCII(4) as base, base64Encode(base) as derived FROM system.numbers limit 2500;
INSERT INTO test2(id, base, derived) SELECT number + 2500 as id, randomPrintableASCII(4) as base, upper(base) as derived FROM system.numbers limit 2500;

DEDUCE TABLE test2 BY derived;

CREATE TABLE test3(id UInt32, base String, derived String) ENGINE = MergeTree ORDER BY id;
INSERT INTO test3(id, base, derived) VALUES
(1, 'bAse1', 'BASE1 base1'),
(2, 'baSe2', 'base2 BASE2');

DEDUCE TABLE test3 BY derived;
