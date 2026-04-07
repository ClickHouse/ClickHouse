SELECT bitShiftLeft(if(number = NULL, '14342', '4242348'), 1) FROM numbers(1);
SELECT bitShiftLeft(if(number = NULL, '14342', '4242348'), 1) FROM numbers(3);
SELECT bitShiftLeft(if(materialize(NULL), '14342', '4242348'), 1) FROM numbers(1);
SELECT bitShiftLeft(if(materialize(1), '123', '123'), 1)  from numbers(1);


-- The next queries are from fuzzer that found the bug:
DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;
CREATE TABLE t0 (vkey UInt32, pkey UInt32, c0 UInt32) engine = TinyLog;
CREATE TABLE t1 (vkey UInt32) ENGINE = AggregatingMergeTree  ORDER BY vkey;
INSERT INTO t0 VALUES (15, 25000, 58);
SELECT ref_5.pkey AS c_2_c2392_6 FROM t0 AS ref_5 WHERE 'J[' < multiIf(ref_5.pkey IN ( SELECT 1 ), bitShiftLeft(multiIf(ref_5.c0 > NULL, '1', ')'), 40), NULL);
DROP TABLE t0;
DROP TABLE t1;

DROP TABLE IF EXISTS t5;
CREATE TABLE t5 (vkey UInt32, pkey UInt32, c18 Float32, c19 UInt32) ENGINE = Log;
INSERT INTO t5 VALUES (3, 13000, 73.90, 83);
SELECT subq_0.pkey as c_1_c1193_15 FROM t5 AS subq_0 WHERE sipHash128(0, subq_0.c18, bitShiftRight(case when false then (sipHash128(subq_0.pkey, subq_0.c18, 'S')) else '1' end, 0)) is not null;
DROP TABLE t5;

