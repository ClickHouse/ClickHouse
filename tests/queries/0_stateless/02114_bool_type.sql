DROP TABLE IF EXISTS bool_test;

CREATE TABLE bool_test (value Bool,f String) ENGINE = Memory;

-- value column shoud have type 'Bool'
SHOW CREATE TABLE bool_test;

INSERT INTO bool_test (value,f) VALUES (false, 'test'), (true , 'test'), (0, 'test'), (1, 'test'), (FALSE, 'test'), (TRUE, 'test');
INSERT INTO bool_test (value,f) FORMAT JSONEachRow {"value":false,"f":"test"}{"value":true,"f":"test"}{"value":0,"f":"test"}{"value":1,"f":"test"}

SELECT value,f FROM bool_test;
SELECT value,f FROM bool_test FORMAT JSONEachRow;
SELECT toUInt64(value),f FROM bool_test;
SELECT value,f FROM bool_test where value > 0;

set bool_true_representation='True';
set bool_false_representation='False';

INSERT INTO bool_test (value,f) FORMAT CSV True,test

INSERT INTO bool_test (value,f) FORMAT TSV False	test

SELECT value,f FROM bool_test order by value FORMAT CSV;
SELECT value,f FROM bool_test order by value FORMAT TSV;

set bool_true_representation='Yes';
set bool_false_representation='No';

INSERT INTO bool_test (value,f) FORMAT CSV Yes,test

INSERT INTO bool_test (value,f) FORMAT TSV No	test

SELECT value,f FROM bool_test order by value FORMAT CSV;
SELECT value,f FROM bool_test order by value FORMAT TSV;

set bool_true_representation='On';
set bool_false_representation='Off';

INSERT INTO bool_test (value,f) FORMAT CSV On,test

INSERT INTO bool_test (value,f) FORMAT TSV Off	test

SELECT value,f FROM bool_test order by value FORMAT CSV;
SELECT value,f FROM bool_test order by value FORMAT TSV;

DROP TABLE IF EXISTS bool_test;

