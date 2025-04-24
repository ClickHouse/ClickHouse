SET enable_hypothesis_deduction = 1;

CREATE TABLE test1(id UInt32, val1 String, val2 String, val3 String, val4 String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE test2(id UInt32, val1 String, val2 String, val3 String, val4 String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE test3(id UInt32, val1 String, val2 String, val3 String, val4 String) ENGINE = MergeTree ORDER BY id;

`INSERT INTO test1(id, val1, val2, val3, val4) SELECT number as id, randomPrintableASCII(7) as val1, 'https://' as val2, '.com' as val3, concat(val2, val1, val3) FROM system.numbers limit 2500;`

DEDUCE TABLE test1 BY val4;

INSERT INTO test1(id, val1, val2, val3, val4) SELECT number + 2500 as id, randomPrintableASCII(7) as val1, 'https://' as val2, '.com' as val3, concat(val3, val2, val1) FROM system.numbers limit 2500;

DEDUCE TABLE test1 BY val4;

INSERT INTO test2(id, val1, val2, val3, val4) SELECT number as id, randomPrintableASCII(7) as val1, 'https://' as val2, '.com' as val3, concat(val3, val2, val1) FROM system.numbers limit 2500;

DEDUCE TABLE test2 BY val4;

INSERT INTO test3(id, val1, val2, val3, val4) SELECT number as id, randomPrintableASCII(7) as val1, 'https://' as val2, '.com' as val3, concat('im a const 1',val3, val2, val1, 'im a const 2') FROM system.numbers limit 2500;

DEDUCE TABLE test3 BY val4;
