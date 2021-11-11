-- Tags: no-parallel
-- Tag no-parallel was added here because it will create table
DROP TABLE IF EXISTS test.bool_test;

CREATE TABLE test.bool_test (value Bool) ENGINE = Memory;

-- value column shoud have type 'Bool'
SHOW CREATE TABLE test.bool_test;

INSERT INTO test.bool_test (value) VALUES ('false'), ('true'), (0), (1);
INSERT INTO test.bool_test (value) FORMAT JSONEachRow {"value":false}{"value":true}{"value":0}{"value":1}

SELECT value FROM test.bool_test;
SELECT value FROM test.bool_test FORMAT JSONEachRow;
SELECT toUInt64(value) FROM test.bool_test;
SELECT value FROM test.bool_test where value > 0;

set bool_format='true_false_camel_case';
INSERT INTO test.bool_test (value) FORMAT CSV True
INSERT INTO test.bool_test (value) FORMAT TSV False
SELECT value FROM test.bool_test order by value FORMAT CSV;
SELECT value FROM test.bool_test order by value FORMAT TSV;

set bool_format='T_F';
INSERT INTO test.bool_test (value) FORMAT CSV T
INSERT INTO test.bool_test (value) FORMAT TSV F
SELECT value FROM test.bool_test order by value FORMAT CSV;
SELECT value FROM test.bool_test order by value FORMAT TSV;

set bool_format='true_false_lower_case';
INSERT INTO test.bool_test (value) FORMAT CSV true
INSERT INTO test.bool_test (value) FORMAT TSV false
SELECT value FROM test.bool_test order by value FORMAT CSV;
SELECT value FROM test.bool_test order by value FORMAT TSV;

set bool_format='Yes_No';
INSERT INTO test.bool_test (value) FORMAT CSV Yes
INSERT INTO test.bool_test (value) FORMAT TSV No
SELECT value FROM test.bool_test order by value FORMAT CSV;
SELECT value FROM test.bool_test order by value FORMAT TSV;

set bool_format='Y_N';
INSERT INTO test.bool_test (value) FORMAT CSV Y
INSERT INTO test.bool_test (value) FORMAT TSV N
SELECT value FROM test.bool_test order by value FORMAT CSV;
SELECT value FROM test.bool_test order by value FORMAT TSV;

set bool_format='On_Off';
INSERT INTO test.bool_test (value) FORMAT CSV On
INSERT INTO test.bool_test (value) FORMAT TSV Off
SELECT value FROM test.bool_test order by value FORMAT CSV;
SELECT value FROM test.bool_test order by value FORMAT TSV;

DROP TABLE IF EXISTS test.bool_test;

