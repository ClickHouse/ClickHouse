SELECT bitShiftLeft(If(number = NULL, '14342', '4242348'), 1) FROM numbers(1);
SELECT bitShiftLeft(If(number = NULL, '14342', '4242348'), 1) FROM numbers(3);
SELECT bitShiftLeft(If(materialize(NULL), '14342', '4242348'), 1) FROM numbers(1);

CREATE TABLE t0 (vkey UInt32, pkey UInt32, c0 UInt32) engine = TinyLog;
CREATE TABLE t1 (vkey UInt32) ENGINE = AggregatingMergeTree  ORDER BY vkey;
INSERT INTO t0 VALUES (15, 25000, 58);
SELECT ref_5.pkey AS c_2_c2392_6 FROM t0 AS ref_5 WHERE 'J[' < multiIf(ref_5.pkey IN ( SELECT 1 ), bitShiftLeft(multiIf(ref_5.c0 > NULL, '1', ')'), 40), NULL);
