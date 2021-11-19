DROP TABLE IF EXISTS bool_test;

CREATE TABLE bool_test (value Bool) ENGINE = Memory;

-- value column shoud have type 'Bool'
SHOW CREATE TABLE bool_test;

INSERT INTO bool_test (value) VALUES ('false'), ('true'), (0), (1);
INSERT INTO bool_test (value) FORMAT JSONEachRow {"value":false}{"value":true}{"value":0}{"value":1}

SELECT value FROM bool_test;
SELECT value FROM bool_test FORMAT JSONEachRow;
SELECT toUInt64(value) FROM bool_test;
SELECT value FROM bool_test where value > 0;

set bool_true_representation='True';
set bool_false_representation='False';
INSERT INTO bool_test (value) FORMAT CSV True
INSERT INTO bool_test (value) FORMAT TSV False
SELECT value FROM bool_test order by value FORMAT CSV;
SELECT value FROM bool_test order by value FORMAT TSV;

set bool_true_representation='Yes';
set bool_false_representation='No';
INSERT INTO bool_test (value) FORMAT CSV Yes
INSERT INTO bool_test (value) FORMAT TSV No
SELECT value FROM bool_test order by value FORMAT CSV;
SELECT value FROM bool_test order by value FORMAT TSV;

set bool_true_representation='On';
set bool_false_representation='Off';
INSERT INTO bool_test (value) FORMAT CSV On
INSERT INTO bool_test (value) FORMAT TSV Off
SELECT value FROM bool_test order by value FORMAT CSV;
SELECT value FROM bool_test order by value FORMAT TSV;

DROP TABLE IF EXISTS bool_test;

